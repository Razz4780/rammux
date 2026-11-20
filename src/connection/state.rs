use crate::{
    connection::{active::Active, downgrade::Downgraded},
    error::{ErrorKind, StreamError},
};

/// Inner state of a [`RammuxConnection`](super::RammuxConnection).
#[allow(clippy::large_enum_variant)]
pub enum ConnState<IO> {
    /// Connection is active.
    Active(Active<IO>),
    /// Connection has failed.
    Poisoned,
    /// Connection was downgraded.
    Downgraded,
}

impl<IO> ConnState<IO> {
    pub fn active_mut(&mut self) -> Result<&mut Active<IO>, ErrorKind> {
        match self {
            Self::Active(active) => Ok(active),
            Self::Poisoned => Err(ErrorKind::Poisoned),
            Self::Downgraded => Err(ErrorKind::AlreadyDowngraded),
        }
    }

    pub fn active(&self) -> Result<&Active<IO>, ErrorKind> {
        match self {
            Self::Active(active) => Ok(active),
            Self::Poisoned => Err(ErrorKind::Poisoned),
            Self::Downgraded => Err(ErrorKind::AlreadyDowngraded),
        }
    }

    /// Starts the downgrade. The streams end with [`StreamError::ConnectionDowngraded`].
    pub fn downgrade(&mut self, term_received: bool) -> Result<Downgraded<IO>, ErrorKind> {
        match std::mem::replace(self, Self::Downgraded) {
            Self::Active(mut active) => {
                active.end_streams(StreamError::ConnectionDowngraded);
                Ok(Downgraded::new(active.into_codec(), term_received))
            },
            Self::Poisoned => {
                *self = Self::Poisoned;
                Err(ErrorKind::Poisoned)
            },
            Self::Downgraded => Err(ErrorKind::AlreadyDowngraded),
        }
    }

    /// The connection failed. The streams end with [`StreamError::ConnectionFailed`].
    pub fn poison(&mut self) {
        if let Self::Active(active) = self {
            active.end_streams(StreamError::ConnectionFailed);
            *self = Self::Poisoned;
        }
    }
}
