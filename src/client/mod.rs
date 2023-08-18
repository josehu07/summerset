//! Summerset's client functionality modules and trait.

mod endpoint;
mod apistub;
mod ctrlstub;

pub use endpoint::{GenericEndpoint, ClientId};
pub use apistub::ClientApiStub;
pub use ctrlstub::ClientCtrlStub;
