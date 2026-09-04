import { prisma } from "../../infrastructure/database/prisma/client";
import { getRabbitChannel } from "../../infrastructure/queue/rabbitmq/connection";
import { publishOutboundMessage } from "../../infrastructure/queue/rabbitmq/publisher";
import { isQueueOpenNow } from "../utils/business-hours";
import { findOrCreateOpenTicket } from "./find-or-create-open-ticket";

interface DeskTicketCreatePayload {
  target: { id: string; [key: string]: unknown };
  whatsappChannel: { id: string; [key: string]: unknown };
  messagingSession: { id: string; [key: string]: unknown };
  agent: { id: string; name: string };
  queueId: string;
  handoffReason?: string;
  assignedUserId?: string;
  skipTransferMessage?: boolean;
}

/// Consome `desk.ticket.create` — publicado pelo AI-Worker no handoff pra
/// atendimento humano (e também pelo Campaign-Worker, quando uma campanha
/// tem routeToQueueId). Idempotente: se já existe ticket aberto pra essa
/// sessão, não faz nada (nem reenvia a transferMessage).
export async function handleDeskTicketCreate(payload: DeskTicketCreatePayload): Promise<void> {
  const organizationId = await resolveOrganizationId(payload);

  const queue = await prisma.queue.findUnique({
    where: { id: payload.queueId },
    select: { businessHoursEnabled: true, businessHoursStart: true, businessHoursEnd: true, businessDays: true },
  });
  if (!queue) {
    throw new Error(`Fila ${payload.queueId} não encontrada para desk.ticket.create.`);
  }

  // Fila fora do horário de atendimento configurado: não cria ticket (fica
  // esperando ninguém até reabrir) — avisa o cliente com outOfHoursMessage,
  // se ativa. Se desativada, o próprio agente de IA já respondeu livremente
  // neste turno (mesma regra das outras mensagens opcionais do Agent) — não
  // há mais nada a enviar aqui.
  if (!isQueueOpenNow(queue)) {
    console.log(
      `[DESK-MSG][handleDeskTicketCreate] queueId=${payload.queueId} fora do horário de atendimento — não cria ticket.`,
    );
    await sendOutOfHoursMessage(payload);
    return;
  }

  await findOrCreateOpenTicket({
    organizationId,
    queueId: payload.queueId,
    targetId: payload.target.id,
    messagingSessionId: payload.messagingSession.id,
    target: payload.target,
    whatsappChannel: payload.whatsappChannel,
    messagingSession: payload.messagingSession,
    agentId: payload.agent.id,
    assignedUserId: payload.assignedUserId,
    skipTransferMessage: payload.skipTransferMessage,
  });
}

async function sendOutOfHoursMessage(payload: DeskTicketCreatePayload): Promise<void> {
  const agent = await prisma.agent.findUnique({
    where: { id: payload.agent.id },
    select: { outOfHoursEnabled: true, outOfHoursMessage: true },
  });
  if (!agent?.outOfHoursEnabled || !agent.outOfHoursMessage) return;

  const channel = await getRabbitChannel();
  await publishOutboundMessage(channel, {
    target: payload.target,
    whatsappChannel: payload.whatsappChannel,
    messagingSession: payload.messagingSession,
    answer: { text: agent.outOfHoursMessage, audio: "", image: "" },
    finishesProcessing: true,
    origin: "SYSTEM",
  });
}

/// organizationId idealmente já vem no payload (target.organizationId), mas
/// como o formato exato montado pelo Inbound-Service pode variar, cai pro
/// lookup via WhatsappChannel (sempre presente e único) como fallback robusto.
async function resolveOrganizationId(payload: DeskTicketCreatePayload): Promise<string> {
  const fromPayload = (payload.target as { organizationId?: string }).organizationId;
  if (fromPayload) return fromPayload;

  const channel = await prisma.whatsappChannel.findUnique({
    where: { id: payload.whatsappChannel.id },
    select: { organizationId: true },
  });
  if (!channel) {
    throw new Error(
      `Não foi possível resolver organizationId para desk.ticket.create (whatsappChannel=${payload.whatsappChannel.id}).`,
    );
  }
  return channel.organizationId;
}
