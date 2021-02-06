import { NatsServer } from "./helpers/launcher.ts";
import { NatsConnection } from "../nats-base-client/types.ts";
import { JetStreamManager } from "../nats-base-client/jetstream.ts";
import { nuid } from "../nats-base-client/nuid.ts";
import { connect } from "../src/connect.ts";

export async function setup(
  conf?: any,
): Promise<{ ns: NatsServer; nc: NatsConnection }> {
  const ns = await NatsServer.start(conf);
  const nc = await connect(
    { port: ns.port, noResponders: true, headers: true },
  );
  return { ns, nc };
}

export async function cleanup(
  ns: NatsServer,
  nc: NatsConnection,
): Promise<void> {
  await nc.close();
  await ns.stop();
}

export async function initStream(
  nc: NatsConnection,
): Promise<{ stream: string; subj: string }> {
  const jsm = await JetStreamManager(nc);
  const stream = nuid.next();
  const subj = `${stream}.A`;
  await jsm.addStream(
    { name: stream, subjects: [subj] },
  );
  return { stream, subj };
}
