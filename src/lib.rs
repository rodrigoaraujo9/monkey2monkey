use tokio::sync::mpsc;

pub mod peer;

type Tx = mpsc::UnboundedSender<String>;
type Rx = mpsc::UnboundedReceiver<String>;

pub const RED: &str = "\x1b[38;2;243;139;168m";
pub const GREEN: &str = "\x1b[38;2;166;227;161m";
pub const YELLOW: &str = "\x1b[38;2;249;226;175m";
pub const BLUE: &str = "\x1b[38;2;137;180;250m";
pub const PURPLE: &str = "\x1b[38;2;203;166;247m";
pub const ORANGE: &str = "\x1b[38;2;250;179;135m";
pub const RESET: &str = "\x1b[0m";
