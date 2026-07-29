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

/// Single source of truth for PipeWire video-format negotiation.
///
/// These are the formats advertised to PipeWire in `stream_params()`, and
/// every one of them **must** have a decode arm in [`video_frame_for`].
///
/// Before 2026-07-29 the two lists silently disagreed: `RGBA` was advertised
/// but had no decode arm, while `xBGR` had a decode arm but was never
/// advertised. A compositor that picked `RGBA` therefore hit the fallback --
/// originally a `panic!` inside a PipeWire callback, later a silent capture
/// shutdown -- despite scap having offered that format itself. Advertising a
/// format we cannot decode is always a bug, so keep these in lockstep;
/// `every_advertised_format_has_a_decode_arm` fails the build if they drift.
const SUPPORTED_VIDEO_FORMATS: [VideoFormat; 4] = [
    VideoFormat::RGB,
    VideoFormat::RGBx,
    VideoFormat::xBGR,
    VideoFormat::BGRx,
];

/// Build the [`VideoFrame`] for a negotiated `format`, or `None` when this
/// engine has no decode arm for it.
///
/// Split out of the `on_process` callback so the advertisement list above can
/// be tested against the dispatch without a live PipeWire stream -- the two
/// drifting apart is precisely the bug this function's test guards.
fn video_frame_for(
    format: VideoFormat,
    display_time: SystemTime,
    width: i32,
    height: i32,
    data: Vec<u8>,
) -> Option<VideoFrame> {
    match format {
        VideoFormat::RGBx => Some(VideoFrame::RGBx(RGBxFrame {
            display_time,
            width,
            height,
            data,
        })),
        VideoFormat::RGB => Some(VideoFrame::RGB(RGBFrame {
            display_time,
            width,
            height,
            data,
        })),
        VideoFormat::xBGR => Some(VideoFrame::XBGR(XBGRFrame {
            display_time,
            width,
            height,
            data,
        })),
        VideoFormat::BGRx => Some(VideoFrame::BGRx(BGRxFrame {
            display_time,
            width,
            height,
            data,
        })),
        _ => None,
    }
}

static CAPTURER_STATE: AtomicU8 = AtomicU8::new(0);
// NOTE: these are process-wide statics; the Linux backend assumes a single active capturer per process.
// Signals pipewire_capturer's main loop to stop early. Set on both a
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
            // Once STREAM_SHOULD_EXIT is set (stream error or disconnected
            // receiver), the outer loop in pipewire_capturer is about to
            // stop anyway -- skip the frame_data copy (the actual
            // per-frame cost here) and everything after it, and just
            // requeue the buffer below.
            if STREAM_SHOULD_EXIT.load(std::sync::atomic::Ordering::Relaxed) {
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
            let send_result = match video_frame_for(
                user_data.format.format(),
                display_time,
                frame_size.width as i32,
                frame_size.height as i32,
                frame_data,
            ) {
                Some(video_frame) => user_data.tx.try_send(Frame::Video(video_frame)),
                None => {
                    // Should be unreachable: PipeWire can only negotiate a
                    // format we advertised, and everything advertised in
                    // SUPPORTED_VIDEO_FORMATS has a decode arm. Kept as a
                    // graceful stop rather than a panic because this runs
                    // inside an FFI-driven callback.
                    if !STREAM_SHOULD_EXIT.swap(true, std::sync::atomic::Ordering::Relaxed) {
                        eprintln!(
                            "Unsupported frame format received: {:?}",
                            user_data.format.format()
                        );
                    }
                    Ok(())
                }
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
    // STREAM_SHOULD_EXIT is a process-wide static; the only other writer
    // is process_callback/state_changed_callback (both set it to `true`).
    // Reset it here, before stream.connect() or any loop iteration, not
    // just before the main capture loop below: connect()'s handshake can
    // plausibly dispatch pipewire callbacks on this same thread before we
    // ever call pw_loop.iterate() ourselves (MainLoop wraps pw_main_loop
    // in an Rc, not Arc -- there is no separate background dispatch
    // thread, so any dispatch that happens before our own iterate() calls
    // must be happening synchronously inside calls like connect()).
    // Resetting only right before the main loop (as an earlier commit did)
    // would silently clear a real pre-start error or disconnect instead of
    // letting it end the loop immediately, as it should.
    STREAM_SHOULD_EXIT.store(false, std::sync::atomic::Ordering::Relaxed);

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
        // Driven by SUPPORTED_VIDEO_FORMATS rather than a second hand-written
        // list, so the set we advertise cannot drift from the set we can
        // actually decode. `RGBA` used to appear here with no decode arm.
        pw::spa::pod::property!(
            FormatProperties::VideoFormat,
            Choice,
            Enum,
            Id,
            SUPPORTED_VIDEO_FORMATS[0],
            SUPPORTED_VIDEO_FORMATS[1],
            SUPPORTED_VIDEO_FORMATS[2],
            SUPPORTED_VIDEO_FORMATS[3],
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
        // Same class of bug as STREAM_SHOULD_EXIT (see the reset in
        // pipewire_capturer): CAPTURER_STATE is a process-wide static, and
        // the only other writer is stop_capture(), which never runs if a
        // previous LinuxCapturer's receiver was dropped instead of properly
        // stopped. Left stale at 1 (or 2), a new instance's background
        // thread would skip its "wait for start_capture()" gate below and
        // start iterating immediately -- capturing before the caller ever
        // called start_capture() on *this* instance. Reset synchronously
        // here, before the background thread exists and before the caller
        // can possibly call start_capture() on the handle this returns, so
        // there's no race with either.
        CAPTURER_STATE.store(0, std::sync::atomic::Ordering::Relaxed);

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

#[cfg(test)]
mod format_negotiation_tests {
    use super::*;

    fn sample(format: VideoFormat) -> Option<VideoFrame> {
        video_frame_for(format, SystemTime::UNIX_EPOCH, 1, 1, vec![0u8; 4])
    }

    /// The regression this whole change exists for: anything we offer to
    /// PipeWire must be something we can actually decode. Adding a format to
    /// `SUPPORTED_VIDEO_FORMATS` without a matching arm in `video_frame_for`
    /// fails here instead of at runtime on a user's compositor.
    #[test]
    fn every_advertised_format_has_a_decode_arm() {
        for format in SUPPORTED_VIDEO_FORMATS {
            assert!(
                sample(format).is_some(),
                "advertised format {format:?} has no decode arm in video_frame_for"
            );
        }
    }

    /// Pins the specific historical bug: `RGBA` was advertised for months
    /// with no decode arm. It must stay out of the advertised set unless and
    /// until a real `RGBA` arm exists.
    #[test]
    fn rgba_is_not_advertised_while_undecodable() {
        assert!(
            sample(VideoFormat::RGBA).is_none(),
            "video_frame_for gained an RGBA arm -- add RGBA to SUPPORTED_VIDEO_FORMATS \
             and delete this test"
        );
        assert!(
            !SUPPORTED_VIDEO_FORMATS.contains(&VideoFormat::RGBA),
            "RGBA is advertised but video_frame_for cannot decode it"
        );
    }

    /// `xBGR` had a decode arm but was never advertised, so it could never be
    /// negotiated. Guards against silently losing it again.
    #[test]
    fn xbgr_is_both_decodable_and_advertised() {
        assert!(sample(VideoFormat::xBGR).is_some());
        assert!(SUPPORTED_VIDEO_FORMATS.contains(&VideoFormat::xBGR));
    }
}
