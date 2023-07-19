use bytes::Bytes;
use color_eyre::eyre::Result;
use num_format::{Locale, ToFormattedString};
use qp2p::{Endpoint, WireMsg};
use std::{
    env,
    net::{Ipv4Addr, SocketAddr},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Once,
    },
    time::Instant,
};

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;

    const BASE_PORT: u16 = 12345;
    let server_addr: SocketAddr = SocketAddr::from((Ipv4Addr::LOCALHOST, BASE_PORT));

    // Collect args
    let args: Vec<String> = env::args().collect();
    let (Ok(num_nodes), Ok(num_loops), Ok(num_msgs), Ok(msg_size)) =
        (args[1].parse::<u16>(), args[2].parse::<usize>(), args[3].parse::<usize>(),  args[4].parse::<usize>()) else {
            panic!("need to pass in args: num_nodes num_loops num_msgs msg_size")
        };

    // Create a peer endpoint
    let (node, mut incoming_conns) = Endpoint::builder()
        .addr((Ipv4Addr::LOCALHOST, BASE_PORT))
        .idle_timeout(60 * 60 * 1_000 /* 3600s = 1h */)
        .server()?;
    println!("set up on {}\n", node.local_addr());

    let once = Once::new();
    let mut timer = Instant::now(); // overwritten in once

    static RECV_COUNTER: AtomicUsize = AtomicUsize::new(0);
    static SEND_COUNTER: AtomicUsize = AtomicUsize::new(0);
    static STOP: AtomicBool = AtomicBool::new(false);
    let server_handle = tokio::task::spawn(async move {
        while let Some((_connection, mut incoming)) = incoming_conns.next().await {
            once.call_once(|| {
                timer = Instant::now();
            });
            // loop over incoming messages
            while let Ok(Some(WireMsg((_, _, msg)))) = incoming.next().await {
                let m = msg.clone();
                std::hint::black_box(m);
                RECV_COUNTER.fetch_add(1, Ordering::Relaxed);
            }

            if STOP.load(Ordering::Relaxed) {
                break;
            }
        }
    });

    // Build all client endpoints
    let nodes: Vec<Endpoint> = (0..num_nodes)
        .map(|i| {
            Endpoint::builder()
                .addr((Ipv4Addr::LOCALHOST, BASE_PORT + 1 + i))
                .idle_timeout(60 * 60 * 1_000 /* 3600s = 1h */)
                .client()
                .unwrap()
        })
        .collect();

    // Connect to and spam peer
    let handles = nodes.into_iter().map(|node| {
        tokio::spawn(async move {
            // Use the same message for everything
            let msg = Bytes::from(vec![0xDA; msg_size]);

            for _ in 0..num_loops {
                // Connect to peer
                let (conn, _incoming) = node.connect_to(&server_addr).await.expect("benchmark");

                // Send message
                for _ in 0..num_msgs {
                    conn.send((Bytes::new(), Bytes::new(), msg.clone()))
                        .await
                        .expect("benchmark");
                }
                SEND_COUNTER.fetch_add(num_msgs, Ordering::Relaxed);
            }
        })
    });

    // Clean up clients and peer, and get final time
    for handle in handles {
        handle.await.unwrap();
    }
    STOP.store(true, Ordering::Relaxed);
    server_handle.await.unwrap();
    let time = timer.elapsed();

    // Calculate load
    let total_conn = num_msgs as u128 * num_nodes as u128;
    let total_msgs = total_conn * num_loops as u128;
    let load_bytes = total_msgs * msg_size as u128;
    let rate_in_mb = load_bytes * 1_000_000 / time.as_micros() as u128 / 1024 / 1024;
    let reqst_rate = total_conn * 1000 / time.as_millis() as u128;
    assert_eq!(RECV_COUNTER.load(Ordering::Relaxed) as u128, total_msgs);
    assert_eq!(
        RECV_COUNTER.load(Ordering::Relaxed),
        SEND_COUNTER.load(Ordering::Relaxed)
    );
    println!(
        "\nprocessed {load_bytes} bytes in {} micros",
        time.as_micros()
    );
    println!("{} MB/s", rate_in_mb.to_formatted_string(&Locale::en));

    println!(
        "\nprocessed {total_conn} messages in {} micros",
        time.as_micros().to_formatted_string(&Locale::en)
    );
    println!("{} reqs/s", reqst_rate.to_formatted_string(&Locale::en));

    Ok(())
}
