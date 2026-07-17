use std::{
    mem::size_of,
    sync::{
        atomic::{AtomicBool, AtomicU8},
        mpsc::{self, sync_channel, SyncSender},
    },
    thread::JoinHandle,
    time::{Duration, SystemTime},
};

use pipewire as pw;
use pw::{
    context::Context,
    main_loop::MainLoop,
    properties::properties,
    spa::{
        self,
        param::{
            format::{FormatProperties, MediaSubtype, MediaType},
            video::VideoFormat,
            ParamType,
        },
        pod::{Pod, Property},
        sys::{
            spa_buffer, spa_meta_header, SPA_META_Header, SPA_PARAM_META_size, SPA_PARAM_META_type,
        },
        utils::{Direction, SpaTypes},
    },
    stream::{StreamRef, StreamState},
};

use crate::{
    capturer::Options,
    frame::{BGRxFrame, Frame, RGBFrame, RGBxFrame, VideoFrame, XBGRFrame},
};

use self::{error::LinCapError, portal::ScreenCastPortal};

mod error;
mod portal;

static CAPTURER_STATE: AtomicU8 = AtomicU8::new(0);
// Signals pipewire_capturer's main loop to stop early. Set both on a
// genuine PipeWire stream error and (see process_callback) when the frame
// receiver has disconnected -- both are "the loop should end now"
// conditions, so one flag models the intent accurately rather than two.
static STREAM_SHOULD_EXIT: AtomicBool = AtomicBool::new(false);

#[derive(Clone)]
struct ListenerUserData {
    pub tx: mpsc::SyncSender<Frame>,
    pub format: spa::param::video::VideoInfoRaw,
}

fn param_changed_callback(
    _stream: &StreamRef,
    user_data: &mut ListenerUserData,
    id: u32,
    param: Option<&Pod>,
) {
    let Some(param) = param else {
        return;
    };
    if id != pw::spa::param::ParamType::Format.as_raw() {
        return;
    }
    let (media_type, media_subtype) = match pw::spa::param::format_utils::parse_format(param) {
        Ok(v) => v,
        Err(_) => return,
    };

    if media_type != MediaType::Video || media_subtype != MediaSubtype::Raw {
        return;
    }

    user_data
        .format
        .parse(param)
        // TODO: Tell library user of the error
        .expect("Failed to parse format parameter");
}

fn state_changed_callback(
    _stream: &StreamRef,
    _user_data: &mut ListenerUserData,
    _old: StreamState,
    new: StreamState,
) {
    match new {
        StreamState::Error(e) => {
            eprintln!("pipewire: State changed to error({e})");
            STREAM_SHOULD_EXIT.store(true, std::sync::atomic::Ordering::Relaxed);
        }
        _ => {}
    }
}

unsafe fn get_timestamp(buffer: *mut spa_buffer) -> i64 {
    let n_metas = (*buffer).n_metas;
    if n_metas > 0 {
        let mut meta_ptr = (*buffer).metas;
        let metas_end = (*buffer).metas.wrapping_add(n_metas as usize);
        while meta_ptr != metas_end {
            if (*meta_ptr).type_ == SPA_META_Header {
                let meta_header: &mut spa_meta_header =
                    &mut *((*meta_ptr).data as *mut spa_meta_header);
                return meta_header.pts;
            }
            meta_ptr = meta_ptr.wrapping_add(1);
        }
        0
    } else {
        0
    }
}

fn process_callback(stream: &StreamRef, user_data: &mut ListenerUserData) {
    let buffer = unsafe { stream.dequeue_raw_buffer() };
    if !buffer.is_null() {
        'outside: {
            let buffer = unsafe { (*buffer).buffer };
            if buffer.is_null() {
                break 'outside;
            }
            let timestamp = unsafe { get_timestamp(buffer) };

            let n_datas = unsafe { (*buffer).n_datas };
            if n_datas < 1 {
                return;
            }
            let frame_size = user_data.format.size();
            let frame_data: Vec<u8> = unsafe {
                std::slice::from_raw_parts(
                    (*(*buffer).datas).data as *mut u8,
                    (*(*buffer).datas).maxsize as usize,
                )
                .to_vec()
            };

            // `timestamp` (from spa_meta_header.pts) is a PipeWire monotonic
            // nanosecond count since an arbitrary reference — not wall-clock.
            // display_time's SystemTime contract is wall-clock, so we use
            // SystemTime::now() here (matches what the macOS and Windows
            // engines do today).  Relative frame ordering survives via
            // channel-send order; sub-millisecond buffer timing is lost.
            let _ = timestamp; // suppress "unused" warning until we wire pts elsewhere
            let display_time = SystemTime::now();

            // `try_send`, not `send`: this callback runs on the same thread
            // that `pipewire_capturer`'s loop uses to poll CAPTURER_STATE
            // between `pw_loop.iterate()` calls. A blocking `send()` on a
            // bounded channel would stall this thread whenever the consumer
            // falls behind, and since `LinuxCapturer::stop_capture()` joins
            // this exact thread, a full channel + a paused consumer would
            // make `stop_capture()` hang forever. Dropping the frame under
            // backpressure (rather than blocking the producer) keeps both
            // the bounded-memory guarantee and a responsive stop path.
            let send_result = match user_data.format.format() {
                VideoFormat::RGBx => {
                    user_data
                        .tx
                        .try_send(Frame::Video(VideoFrame::RGBx(RGBxFrame {
                            display_time,
                            width: frame_size.width as i32,
                            height: frame_size.height as i32,
                            data: frame_data,
                        })))
                }
                VideoFormat::RGB => {
                    user_data
                        .tx
                        .try_send(Frame::Video(VideoFrame::RGB(RGBFrame {
                            display_time,
                            width: frame_size.width as i32,
                            height: frame_size.height as i32,
                            data: frame_data,
                        })))
                }
                VideoFormat::xBGR => {
                    user_data
                        .tx
                        .try_send(Frame::Video(VideoFrame::XBGR(XBGRFrame {
                            display_time,
                            width: frame_size.width as i32,
                            height: frame_size.height as i32,
                            data: frame_data,
                        })))
                }
                VideoFormat::BGRx => {
                    user_data
                        .tx
                        .try_send(Frame::Video(VideoFrame::BGRx(BGRxFrame {
                            display_time,
                            width: frame_size.width as i32,
                            height: frame_size.height as i32,
                            data: frame_data,
                        })))
                }
                _ => panic!("Unsupported frame format received"),
            };

            if let Err(mpsc::TrySendError::Disconnected(_)) = send_result {
                // The consumer (Capturer/rx) is gone, e.g. dropped without
                // calling stop_capture(). Signal the main loop above to
                // exit instead of spinning pw_loop.iterate() forever on a
                // stream nobody will ever read from, and log it once (swap
                // instead of store) rather than once per incoming frame.
                if !STREAM_SHOULD_EXIT.swap(true, std::sync::atomic::Ordering::Relaxed) {
                    eprintln!("Frame receiver disconnected");
                }
            }
            // TrySendError::Full is a silent intentional drop under
            // backpressure — see comment above.
        }
    } else {
        eprintln!("Out of buffers");
    }

    unsafe { stream.queue_raw_buffer(buffer) };
}

// TODO: Format negotiation
fn pipewire_capturer(
    options: Options,
    tx: mpsc::SyncSender<Frame>,
    ready_sender: &SyncSender<bool>,
    stream_id: u32,
) -> Result<(), LinCapError> {
    pw::init();

    let mainloop = MainLoop::new(None)?;
    let context = Context::new(&mainloop)?;
    let core = context.connect(None)?;

    let user_data = ListenerUserData {
        tx,
        format: Default::default(),
    };

    let stream = pw::stream::Stream::new(
        &core,
        "scap",
        properties! {
            *pw::keys::MEDIA_TYPE => "Video",
            *pw::keys::MEDIA_CATEGORY => "Capture",
            *pw::keys::MEDIA_ROLE => "Screen",
        },
    )?;

    let _listener = stream
        .add_local_listener_with_user_data(user_data.clone())
        .state_changed(state_changed_callback)
        .param_changed(param_changed_callback)
        .process(process_callback)
        .register()?;

    let obj = pw::spa::pod::object!(
        pw::spa::utils::SpaTypes::ObjectParamFormat,
        pw::spa::param::ParamType::EnumFormat,
        pw::spa::pod::property!(FormatProperties::MediaType, Id, MediaType::Video),
        pw::spa::pod::property!(FormatProperties::MediaSubtype, Id, MediaSubtype::Raw),
        pw::spa::pod::property!(
            FormatProperties::VideoFormat,
            Choice,
            Enum,
            Id,
            pw::spa::param::video::VideoFormat::RGB,
            pw::spa::param::video::VideoFormat::RGBA,
            pw::spa::param::video::VideoFormat::RGBx,
            pw::spa::param::video::VideoFormat::BGRx,
        ),
        pw::spa::pod::property!(
            FormatProperties::VideoSize,
            Choice,
            Range,
            Rectangle,
            pw::spa::utils::Rectangle {
                // Default
                width: 128,
                height: 128,
            },
            pw::spa::utils::Rectangle {
                // Min
                width: 1,
                height: 1,
            },
            pw::spa::utils::Rectangle {
                // Max
                width: 4096,
                height: 4096,
            }
        ),
        pw::spa::pod::property!(
            FormatProperties::VideoMaxFramerate,
            Fraction,
            pw::spa::utils::Fraction {
                num: options.fps,
                denom: 1
            }
        ),
    );

    let metas_obj = pw::spa::pod::object!(
        SpaTypes::ObjectParamMeta,
        ParamType::Meta,
        Property::new(
            SPA_PARAM_META_type,
            pw::spa::pod::Value::Id(pw::spa::utils::Id(SPA_META_Header))
        ),
        Property::new(
            SPA_PARAM_META_size,
            pw::spa::pod::Value::Int(size_of::<pw::spa::sys::spa_meta_header>() as i32)
        ),
    );

    let values: Vec<u8> = pw::spa::pod::serialize::PodSerializer::serialize(
        std::io::Cursor::new(Vec::new()),
        &pw::spa::pod::Value::Object(obj),
    )?
    .0
    .into_inner();
    let metas_values: Vec<u8> = pw::spa::pod::serialize::PodSerializer::serialize(
        std::io::Cursor::new(Vec::new()),
        &pw::spa::pod::Value::Object(metas_obj),
    )?
    .0
    .into_inner();

    let mut params = [
        pw::spa::pod::Pod::from_bytes(&values).unwrap(),
        pw::spa::pod::Pod::from_bytes(&metas_values).unwrap(),
    ];

    stream.connect(
        Direction::Input,
        Some(stream_id),
        pw::stream::StreamFlags::AUTOCONNECT | pw::stream::StreamFlags::MAP_BUFFERS,
        &mut params,
    )?;

    ready_sender.send(true)?;

    while CAPTURER_STATE.load(std::sync::atomic::Ordering::Relaxed) == 0 {
        std::thread::sleep(Duration::from_millis(10));
    }

    let pw_loop = mainloop.loop_();

    // STREAM_SHOULD_EXIT is a process-wide static: if a previous
    // LinuxCapturer's receiver was dropped without stop_capture() being
    // called, the flag is left set to `true` (stop_capture() is the only
    // other place that resets it, and it never ran). Reset it here, right
    // as this session's loop actually starts, so a new capture session
    // doesn't inherit a stale exit signal and return immediately without
    // ever iterating.
    STREAM_SHOULD_EXIT.store(false, std::sync::atomic::Ordering::Relaxed);

    // User has called Capturer::start() and we start the main loop
    while CAPTURER_STATE.load(std::sync::atomic::Ordering::Relaxed) == 1
        && /* Exit early on a PipeWire stream error or a disconnected frame receiver. TODO: tell user that we exited */
          !STREAM_SHOULD_EXIT.load(std::sync::atomic::Ordering::Relaxed)
    {
        pw_loop.iterate(Duration::from_millis(100));
    }

    Ok(())
}

pub struct LinuxCapturer {
    capturer_join_handle: Option<JoinHandle<Result<(), LinCapError>>>,
    // The pipewire stream is deleted when the connection is dropped.
    // That's why we keep it alive
    _connection: dbus::blocking::Connection,
}

impl LinuxCapturer {
    // TODO: Error handling
    pub fn new(options: &Options, tx: mpsc::SyncSender<Frame>) -> Self {
        let connection =
            dbus::blocking::Connection::new_session().expect("Failed to create dbus connection");
        let stream_id = ScreenCastPortal::new(&connection)
            .show_cursor(options.show_cursor)
            .expect("Unsupported cursor mode")
            .create_stream()
            .expect("Failed to get screencast stream")
            .pw_node_id();

        // TODO: Fix this hack
        let options = options.clone();
        let (ready_sender, ready_recv) = sync_channel(1);
        let capturer_join_handle = std::thread::spawn(move || {
            let res = pipewire_capturer(options, tx, &ready_sender, stream_id);
            if res.is_err() {
                ready_sender.send(false)?;
            }
            res
        });

        if !ready_recv.recv().expect("Failed to receive") {
            panic!("Failed to setup capturer");
        }

        Self {
            capturer_join_handle: Some(capturer_join_handle),
            _connection: connection,
        }
    }

    pub fn start_capture(&self) {
        CAPTURER_STATE.store(1, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn stop_capture(&mut self) {
        CAPTURER_STATE.store(2, std::sync::atomic::Ordering::Relaxed);
        if let Some(handle) = self.capturer_join_handle.take() {
            if let Err(e) = handle.join().expect("Failed to join capturer thread") {
                eprintln!("Error occured capturing: {e}");
            }
        }
        CAPTURER_STATE.store(0, std::sync::atomic::Ordering::Relaxed);
        STREAM_SHOULD_EXIT.store(false, std::sync::atomic::Ordering::Relaxed);
    }
}

pub fn create_capturer(options: &Options, tx: mpsc::SyncSender<Frame>) -> LinuxCapturer {
    LinuxCapturer::new(options, tx)
}
