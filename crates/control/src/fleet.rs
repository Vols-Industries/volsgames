use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::middleware;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use dashmap::DashMap;
use serde::Serialize;
use serde_json::Value;

use crate::{auth, problem};

#[derive(Clone)]
pub struct Node {
    pub url: String,
    pub key: String,
}

pub struct Fleet {
    nodes: Vec<Node>,
    api_key_hash: [u8; 32],
    agent: ureq::Agent,
    placed: DashMap<String, usize>,
}

#[derive(Serialize)]
pub struct NodeView {
    pub url: String,
    pub up: bool,
    pub rooms: usize,
    pub players: u64,
}

struct Reply {
    status: u16,
    body: Value,
}

impl Fleet {
    pub fn new(nodes: Vec<Node>, api_key_hash: [u8; 32]) -> Self {
        let nodes = nodes.into_iter().map(|n| Node { url: n.url.trim_end_matches('/').to_string(), key: n.key }).collect();
        let agent =
            ureq::Agent::new_with_config(ureq::config::Config::builder().http_status_as_error(false).timeout_global(Some(Duration::from_secs(3))).build());
        Fleet { nodes, api_key_hash, agent, placed: DashMap::new() }
    }

    async fn call(&self, node: usize, method: &'static str, path: String, body: Option<Value>) -> Option<Reply> {
        let Node { url, key } = self.nodes[node].clone();
        let agent = self.agent.clone();
        tokio::task::spawn_blocking(move || {
            let url = format!("{url}{path}");
            let auth = format!("Bearer {key}");
            let resp = match (method, body) {
                ("POST", Some(b)) => agent.post(&url).header("authorization", &auth).send_json(b),
                ("DELETE", _) => agent.delete(&url).header("authorization", &auth).call(),
                _ => agent.get(&url).header("authorization", &auth).call(),
            }
            .ok()?;
            let status = resp.status().as_u16();
            let text = resp.into_body().read_to_string().ok()?;
            Some(Reply { status, body: serde_json::from_str(&text).unwrap_or(Value::Null) })
        })
        .await
        .ok()
        .flatten()
    }

    async fn probe(&self) -> Vec<NodeView> {
        let mut out = Vec::with_capacity(self.nodes.len());
        for (i, n) in self.nodes.iter().enumerate() {
            let rooms = self.call(i, "GET", "/v1/rooms".into(), None).await.filter(|r| r.status == 200).and_then(|r| r.body.as_array().cloned());
            let up = rooms.is_some();
            let rooms = rooms.unwrap_or_default();
            let players = rooms.iter().map(|v| v["players"].as_u64().unwrap_or(0)).sum();
            out.push(NodeView { url: n.url.clone(), up, rooms: rooms.len(), players });
        }
        out
    }

    async fn pick(&self) -> Option<usize> {
        self.probe().await.iter().enumerate().filter(|(_, n)| n.up).min_by_key(|(_, n)| n.rooms as u64 + n.players).map(|(i, _)| i)
    }

    async fn locate(&self, room: &str) -> Option<usize> {
        if let Some(i) = self.placed.get(room) {
            return Some(*i);
        }
        for i in 0..self.nodes.len() {
            if self.call(i, "GET", format!("/v1/rooms/{room}"), None).await.is_some_and(|r| r.status == 200) {
                self.placed.insert(room.to_string(), i);
                return Some(i);
            }
        }
        None
    }

    fn tag(&self, node: usize, mut v: Value) -> Value {
        if v.is_object() {
            v["node"] = Value::String(self.nodes[node].url.clone());
        }
        v
    }

    async fn forward(&self, room: &str, method: &'static str, path: String, body: Option<Value>) -> Response {
        let Some(i) = self.locate(room).await else {
            return problem(StatusCode::NOT_FOUND, "no_room", "no such room");
        };
        let Some(r) = self.call(i, method, path, body).await else {
            return problem(StatusCode::SERVICE_UNAVAILABLE, "node_down", "the node holding this room is unreachable");
        };
        if r.status == 404 || r.status == 204 {
            self.placed.remove(room);
        }
        relay(r.status, self.tag(i, r.body))
    }
}

fn relay(status: u16, body: Value) -> Response {
    let status = StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_GATEWAY);
    if status == StatusCode::NO_CONTENT {
        return status.into_response();
    }
    (status, Json(body)).into_response()
}

async fn create_room(State(f): State<Arc<Fleet>>, Json(req): Json<Value>) -> Response {
    let Some(i) = f.pick().await else {
        return problem(StatusCode::SERVICE_UNAVAILABLE, "no_node", "no node is reachable");
    };
    let Some(r) = f.call(i, "POST", "/v1/rooms".into(), Some(req)).await else {
        return problem(StatusCode::SERVICE_UNAVAILABLE, "node_down", "the chosen node went away");
    };
    if r.status == 201
        && let Some(room) = r.body["room"].as_str()
    {
        f.placed.insert(room.to_string(), i);
        tracing::info!(room, node = i, "room place");
    }
    relay(r.status, f.tag(i, r.body))
}

async fn list_rooms(State(f): State<Arc<Fleet>>) -> Response {
    let mut all = Vec::new();
    for i in 0..f.nodes.len() {
        if let Some(r) = f.call(i, "GET", "/v1/rooms".into(), None).await
            && let Some(rooms) = r.body.as_array()
        {
            all.extend(rooms.iter().cloned().map(|v| f.tag(i, v)));
        }
    }
    Json(all).into_response()
}

async fn get_room(State(f): State<Arc<Fleet>>, Path(room): Path<String>) -> Response {
    f.forward(&room, "GET", format!("/v1/rooms/{room}"), None).await
}

async fn delete_room(State(f): State<Arc<Fleet>>, Path(room): Path<String>) -> Response {
    f.forward(&room, "DELETE", format!("/v1/rooms/{room}"), None).await
}

async fn mint_token(State(f): State<Arc<Fleet>>, Path(room): Path<String>, Json(req): Json<Value>) -> Response {
    f.forward(&room, "POST", format!("/v1/rooms/{room}/tokens"), Some(req)).await
}

async fn nodes(State(f): State<Arc<Fleet>>) -> Response {
    Json(f.probe().await).into_response()
}

async fn healthz() -> &'static str {
    "ok"
}

pub fn router(f: Arc<Fleet>) -> Router {
    let private = Router::new()
        .route("/v1/rooms", post(create_room).get(list_rooms))
        .route("/v1/rooms/{room}", get(get_room).delete(delete_room))
        .route("/v1/rooms/{room}/tokens", post(mint_token))
        .route("/v1/nodes", get(nodes))
        .route_layer(middleware::from_fn_with_state(f.api_key_hash, auth));
    Router::new().route("/healthz", get(healthz)).merge(private).with_state(f)
}

pub async fn serve(f: Arc<Fleet>, addr: SocketAddr) -> std::io::Result<(SocketAddr, tokio::task::JoinHandle<()>)> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let addr = listener.local_addr()?;
    let app = router(f);
    let task = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    Ok((addr, task))
}
