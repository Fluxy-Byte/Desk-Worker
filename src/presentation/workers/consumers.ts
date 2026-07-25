import type { Channel, ConsumeMessage } from "amqplib";
import { assertQueueWithDlq } from "../../infrastructure/queue/rabbitmq/connection";
import { handleDeskMessageInbound } from "../../application/use-cases/handle-desk-message-inbound";
import { handleDeskMessageOutbound } from "../../application/use-cases/handle-desk-message-outbound";
import { handleDeskMessageSent } from "../../application/use-cases/handle-desk-message-sent";
import { handleDeskTicketCreate } from "../../application/use-cases/handle-desk-ticket-create";

const QUEUE_DESK_TICKET_CREATE = "desk.ticket.create";
const QUEUE_DESK_MESSAGE_INBOUND = "desk.message.inbound";
const QUEUE_DESK_MESSAGE_OUTBOUND = "desk.message.outbound";
const QUEUE_DESK_MESSAGE_SENT = "desk.message.sent";

// eslint-disable-next-line @typescript-eslint/no-explicit-any
async function consume(channel: Channel, queue: string, handler: (payload: any) => Promise<void>): Promise<void> {
  await assertQueueWithDlq(channel, queue);
  await channel.prefetch(1);

  channel.consume(queue, async (msg: ConsumeMessage | null) => {
    if (!msg) return;

    try {
      const payload = JSON.parse(msg.content.toString());
      await handler(payload);
      channel.ack(msg);
    } catch (error) {
      console.error(`Erro ao processar mensagem da fila ${queue}:`, error);
      channel.nack(msg, false, false);
    }
  });

  console.log(`Aguardando mensagens na fila ${queue}`);
}

export async function startConsumers(channel: Channel): Promise<void> {
  await consume(channel, QUEUE_DESK_TICKET_CREATE, handleDeskTicketCreate);
  await consume(channel, QUEUE_DESK_MESSAGE_INBOUND, handleDeskMessageInbound);
  await consume(channel, QUEUE_DESK_MESSAGE_OUTBOUND, handleDeskMessageOutbound);
  await consume(channel, QUEUE_DESK_MESSAGE_SENT, handleDeskMessageSent);
}
