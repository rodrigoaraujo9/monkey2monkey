use tokio::sync::mpsc;

pub mod peer;
// define the types common to all
type Tx = mpsc::UnboundedSender<String>;
type Rx = mpsc::UnboundedReceiver<String>;
