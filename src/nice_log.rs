#![macro_use]

#[macro_export]
macro_rules! panic {
    ($($args:tt)+) => {{
        log::error!("{}\n --> {}:{}:{}", format_args!($($args)*), file!(), line!(), column!());
        std::panic!("{}\n --> {}:{}:{}", format_args!($($args)*), file!(), line!(), column!());
    }};
}

#[macro_export]
macro_rules! error {
    ($($args:tt)+) => {{
        log::error!("{}\n --> {}:{}:{}", format_args!($($args)*), file!(), line!(), column!());
    }};
}

#[macro_export]
macro_rules! warn {
    ($($args:tt)+) => {{
        log::warn!("{}\n --> {}:{}:{}", format_args!($($args)*), file!(), line!(), column!());
    }};
}

#[macro_export]
macro_rules! debug {
    ($($args:tt)+) => {{
        log::debug!("{}\n --> {}:{}:{}", format_args!($($args)*), file!(), line!(), column!());
    }};
}

#[macro_export]
macro_rules! trace {
    ($($args:tt)+) => {{
        log::trace!("{}\n --> {}:{}:{}", format_args!($($args)*), file!(), line!(), column!());
    }};
}

#[macro_export]
macro_rules! info {
    ($($args:tt)+) => {{
        log::info!("{}\n --> {}:{}:{}", format_args!($($args)*), file!(), line!(), column!());
    }};
}

#[macro_export]
macro_rules! println {
    ($($args:tt)+) => {{
        log::info!("{}\n --> {}:{}:{}", format_args!($($args)*), file!(), line!(), column!());
        std::println!($($args)*);
    }};
}

