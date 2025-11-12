#[allow(dead_code)]
pub const RED: &str = "\x1b[38;2;243;139;168m"; // #f38ba8
#[allow(dead_code)]
pub const GREEN: &str = "\x1b[38;2;166;227;161m"; // #a6e3a1
#[allow(dead_code)]
pub const YELLOW: &str = "\x1b[38;2;249;226;175m"; // #f9e2af
#[allow(dead_code)]
pub const BLUE: &str = "\x1b[38;2;137;180;250m"; // #89b4fa
#[allow(dead_code)]
pub const PURPLE: &str = "\x1b[38;2;203;166;247m"; // #cba6f7
#[allow(dead_code)]
pub const AQUA: &str = "\x1b[38;2;148;226;213m"; // #94e2d5
#[allow(dead_code)]
pub const ORANGE: &str = "\x1b[38;2;250;179;135m"; // #fab387
#[allow(dead_code)]
pub const RESET: &str = "\x1b[0m";

#[macro_export]
macro_rules! __print_tag {
    (stdout: $tag:expr, $color:expr, $($arg:tt)*) => {{
        println!("{}[{}]{} {}", $color, $tag, $crate::logui::RESET, format!($($arg)*));
    }};
    (stderr: $tag:expr, $color:expr, $($arg:tt)*) => {{
        eprintln!("{}[{}]{} {}", $color, $tag, $crate::logui::RESET, format!($($arg)*));
    }};
}

#[macro_export]
macro_rules! info   { ($($arg:tt)*) => { $crate::__print_tag!(stdout: "Info",   $crate::logui::BLUE,   $($arg)* ) } }
#[macro_export]
macro_rules! ok     { ($($arg:tt)*) => { $crate::__print_tag!(stdout: "OK",     $crate::logui::GREEN,  $($arg)* ) } }
#[macro_export]
macro_rules! warn   { ($($arg:tt)*) => { $crate::__print_tag!(stdout: "Warn",   $crate::logui::ORANGE, $($arg)* ) } }
#[macro_export]
macro_rules! error  { ($($arg:tt)*) => { $crate::__print_tag!(stderr: "Error",  $crate::logui::RED,    $($arg)* ) } }
#[macro_export]
macro_rules! comm   { ($($arg:tt)*) => { $crate::__print_tag!(stdout: "Comm",   $crate::logui::AQUA,   $($arg)* ) } }
#[macro_export]
macro_rules! peers  { ($($arg:tt)*) => { $crate::__print_tag!(stdout: "Peers",  $crate::logui::PURPLE, $($arg)* ) } }

#[macro_export]
macro_rules! banana { ($($arg:tt)*) => { $crate::__print_tag!(stdout: "Banana", $crate::logui::YELLOW, $($arg)* ) } }

#[macro_export]
macro_rules! prompt { ($($arg:tt)*) => { $crate::__print_tag!(stdout: "Input",  $crate::logui::BLUE,   $($arg)* ) } }
