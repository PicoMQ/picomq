use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::layer::{Context, Layer};

pub type LogCallback = extern "C" fn(
    level: u8,
    target_ptr: *const u8,
    target_len: usize,
    message_ptr: *const u8,
    message_len: usize,
);

pub struct CallbackLayer {
    callback: LogCallback,
}

impl CallbackLayer {
    pub fn new(callback: LogCallback) -> Self {
        Self { callback }
    }
}

impl<S: Subscriber> Layer<S> for CallbackLayer {
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let level = match *event.metadata().level() {
            Level::TRACE => 0u8,
            Level::DEBUG => 1,
            Level::INFO => 2,
            Level::WARN => 3,
            Level::ERROR => 4,
        };

        let target = event.metadata().target();

        let mut visitor = MessageVisitor::default();
        event.record(&mut visitor);
        let message = visitor.message;

        (self.callback)(
            level,
            target.as_ptr(),
            target.len(),
            message.as_ptr(),
            message.len(),
        );
    }
}

#[derive(Default)]
struct MessageVisitor {
    message: String,
}

impl Visit for MessageVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" || self.message.is_empty() {
            self.message = value.to_string();
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" || self.message.is_empty() {
            self.message = format!("{:?}", value);
        }
    }
}
