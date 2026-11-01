use bytes::{Bytes, BytesMut};
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;
use volsgames_proto::{Datagram, Lane, StreamBuffer};
use web_transport_quinn::{RecvStream, SendStream, Session};

#[allow(clippy::large_enum_variant)]
pub enum Rx {
    Quic { session: Session, stream: RecvStream, buf: StreamBuffer, chunk: Vec<u8> },
    Ws(SplitStream<WebSocketStream<TcpStream>>),
}

#[allow(clippy::large_enum_variant)]
pub enum Tx {
    Quic { session: Session, stream: SendStream },
    Ws(SplitSink<WebSocketStream<TcpStream>, Message>),
}

pub enum Frame {
    Record(Bytes),
    Datagram(Bytes),
}

impl Rx {
    pub async fn next(&mut self) -> Option<Frame> {
        match self {
            Rx::Quic { session, stream, buf, chunk } => loop {
                if let Some(r) = buf.pop() {
                    return Some(Frame::Record(r));
                }
                tokio::select! {
                    d = session.read_datagram() => return d.ok().map(Frame::Datagram),
                    n = stream.read(chunk) => match n {
                        Ok(Some(n)) => buf.push(&chunk[..n]),
                        _ => return None,
                    },
                }
            },
            Rx::Ws(s) => loop {
                match s.next().await {
                    Some(Ok(Message::Binary(mut b))) => {
                        if b.is_empty() {
                            return None;
                        }
                        let lane = b.split_to(1)[0];
                        return match lane {
                            0 => {
                                let mut sb = StreamBuffer::default();
                                sb.push(&b);
                                return sb.pop().map(Frame::Record);
                            }
                            1 => Some(Frame::Datagram(b)),
                            _ => None,
                        };
                    }
                    Some(Ok(Message::Ping(_) | Message::Pong(_))) => continue,
                    _ => return None,
                }
            },
        }
    }
}

impl Tx {
    pub async fn write(&mut self, lane: Lane, seq: u16, body: Bytes) -> bool {
        match (self, lane) {
            (Tx::Quic { stream, .. }, Lane::Reliable) => stream.write_all(&body).await.is_ok(),
            (Tx::Quic { session, .. }, Lane::Unreliable) => {
                let frame = Datagram::frame(seq, &body);
                if frame.len() > session.max_datagram_size() {
                    return true;
                }
                session.send_datagram(frame).is_ok()
            }
            (Tx::Ws(sink), lane) => {
                let mut out = BytesMut::with_capacity(body.len() + 3);
                match lane {
                    Lane::Reliable => out.extend_from_slice(&[0]),
                    Lane::Unreliable => {
                        out.extend_from_slice(&[1]);
                        out.extend_from_slice(&seq.to_le_bytes());
                    }
                }
                out.extend_from_slice(&body);
                sink.send(Message::Binary(out.freeze())).await.is_ok()
            }
        }
    }

    pub async fn close(self) {
        match self {
            Tx::Quic { mut stream, session } => {
                let _ = stream.finish();
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                session.close(0, b"bye");
            }
            Tx::Ws(mut sink) => {
                let _ = sink.send(Message::Close(None)).await;
            }
        }
    }
}
