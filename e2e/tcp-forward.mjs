// Minimal TCP forwarder for dev wiring: exposes a loopback-bound service to
// docker containers via host-gateway. Zero dependencies.
//   node e2e/tcp-forward.mjs <listenPort> <targetPort> [targetHost]
import { createServer, connect } from 'node:net';

const [listenPort, targetPort, targetHost = '127.0.0.1'] = process.argv.slice(2);
if (!listenPort || !targetPort) {
  console.error('usage: tcp-forward.mjs <listenPort> <targetPort> [targetHost]');
  process.exit(1);
}
createServer(sock => {
  const up = connect(Number(targetPort), targetHost);
  sock.pipe(up).pipe(sock);
  const drop = () => { sock.destroy(); up.destroy(); };
  sock.on('error', drop);
  up.on('error', drop);
}).listen(Number(listenPort), '0.0.0.0', () =>
  console.log(`forwarding 0.0.0.0:${listenPort} -> ${targetHost}:${targetPort}`));
