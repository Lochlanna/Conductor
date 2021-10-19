//! The super crate for the shared components of the conductor stack. This crate is to be used to
//! combine functionality from multiple conductor crates which are used together by producers and
//! reactors.
//!
//! Conductor is a software stack for IOT and automation. It's primary goal is keeping it simple.
//! Born out of the realisation that there was no system that gives powerful IOT functionality while
//! and provides useful automation. Conductor is designed to be dead simple both to deploy and
//! develop your own systems for.
//!
//! Producers are custom build applications that produce data for the conductor system. It could be
//! a sensor on an Arduino. A web scraper scraping weather or news or something as simple as the
//! system CPU usage. Producers are able to create and store data with any schema on the Conductor
//! system.
//!
//! Reactors are devices or programs which are able to do things. They register a set of actions with
//! the conductor server and then listen for one of those actions to be sent to them to be performed.
//! The conductor server is responsible for providing the automation capability by supporting triggers
//! based of producer data which can trigger on or more actions to be sent to a reactor.|
//!
//! While simplicity and ease of use is the primary goal being small and lightweight enough to comfortably
//! run the server on a raspberry pi and have Arduino's be either producers or reactors is also a top
//! priority.
pub use conductor_derive as derive;
pub use conductor_shared::*;
