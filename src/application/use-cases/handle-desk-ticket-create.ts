import { prisma } from "../../infrastructure/database/prisma/client";
import { findOrCreateOpenTicket } from "./find-or-create-open-ticket";

interface DeskTicketCreatePayload {
  target: { id: string; [key: string]: unknown };
  whatsappChannel: { id: string; [key: string]: unknown };
  messagingSession: { id: string; [key: string]: unknown };
  agent: { id: string; name: string };
  queueId: string;
  handoffReason?: string;
  assignedUserId?: string;
}

/// Consome `desk.ticket.create` — publicado pelo AI-Worker no handoff pra
/// atendimento humano. Idempotente: se já existe ticket aberto pra essa
/// sessão, não faz nada (nem reenvia a transferMessage).
export async function handleDeskTicketCreate(payload: DeskTicketCreatePayload): Promise<void> {
  const organizationId = await resolveOrganizationId(payload);

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
