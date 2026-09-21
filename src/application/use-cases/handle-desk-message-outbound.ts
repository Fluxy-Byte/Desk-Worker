import { prisma } from "../../infrastructure/database/prisma/client";
import { getLastInboundMessage } from "../../infrastructure/cache/redis/last-inbound-message";
import { getRabbitChannel } from "../../infrastructure/queue/rabbitmq/connection";
import { publishMarkRead, publishOutboundMessage } from "../../infrastructure/queue/rabbitmq/publisher";
import { publishDeskEvent } from "../../infrastructure/pubsub/desk-events";

interface DeskMessageOutboundPayload {
  ticketId: string;
  text: string;
  attendantUserId: string;
  messageType?: "TEXT" | "AUDIO" | "IMAGE" | "DOCUMENT" | "STICKER" | "VIDEO";
  mediaUrl?: string;
}

/// Consome `desk.message.outbound` — publicado pelo Desk-API quando um
/// atendente envia uma mensagem. mongoMessageId começa null e é preenchido
/// depois via `desk.message.sent` (reconciliação, ver handle-desk-message-sent).
///
/// Sem MessageLog aqui de propósito: esta mensagem nasce agora, sem
/// mongoMessageId/externalMessageId nem qualquer referência a uma mensagem
/// inbound — não há id nenhum pra registrar ainda. O início do rastreamento
/// dela só é possível no Outbound-Worker, quando o Mongo gera o
/// mongoMessageId (ver send-outbound-message.ts).
export async function handleDeskMessageOutbound(payload: DeskMessageOutboundPayload): Promise<void> {
  console.log(`[DESK-MSG][handleDeskMessageOutbound] início — ticketId=${payload.ticketId} attendantUserId=${payload.attendantUserId}`);

  const ticket = await prisma.ticket.findUnique({
    where: { id: payload.ticketId },
    include: {
      target: { include: { whatsappChannel: true } },
      messagingSession: true,
      queue: { select: { serviceIsland: { select: { useAttendantSignature: true } } } },
    },
  });

  if (!ticket) {
    console.error(`[DESK-MSG][handleDeskMessageOutbound] ticket ${payload.ticketId} não encontrado — ignorando.`);
    return;
  }

  if (ticket.status !== "IN_PROGRESS") {
    console.error(
      `[DESK-MSG][handleDeskMessageOutbound] ticket ${payload.ticketId} não está IN_PROGRESS (status=${ticket.status}) — ignorando.`,
    );
    return;
  }

  console.log(
    `[DESK-MSG][handleDeskMessageOutbound] ticketId=${payload.ticketId} target=${ticket.target.id} whatsappChannelId=${ticket.target.whatsappChannel.id} — gravando ticketMessage`,
  );

  const channel = await getRabbitChannel();

  // Assim que o atendente responde, marca a última mensagem do cliente como
  // lida e liga o "digitando..." — best-effort, sem externalMessageId
  // guardado (sessão sem mensagem inbound recente, TTL expirado etc.) só
  // segue sem o indicador.
  const lastInboundExternalMessageId = await getLastInboundMessage(ticket.messagingSessionId);
  if (lastInboundExternalMessageId) {
    await publishMarkRead(channel, {
      whatsappChannelId: ticket.target.whatsappChannel.id,
      phoneNumberId: ticket.target.whatsappChannel.phoneNumberId,
      externalMessageId: lastInboundExternalMessageId,
      typingIndicator: true,
    });
  }

  // Só metadado (quem/quando/tipo) — o conteúdo (texto/mediaUrl) vive no
  // documento Mongo, gravado pelo Outbound-Worker e reconciliado depois via
  // desk.message.sent (ver handle-desk-message-sent).
  await prisma.ticketMessage.create({
    data: {
      ticketId: ticket.id,
      senderType: "ATTENDANT",
      senderUserId: payload.attendantUserId,
      messageType: payload.messageType ?? "TEXT",
      mongoMessageId: null,
    },
  });

  await prisma.messagingSession.update({
    where: { id: ticket.messagingSessionId },
    data: { lastAttendantMessageAt: new Date() },
  });

  // Avisa o front em tempo real assim que a TicketMessage existe — sem isso o
  // balão otimista de "enviando..." só some quando algum OUTRO evento (resposta
  // do cliente, tick de status) força um refetch por coincidência.
  await publishDeskEvent({
    type: "ticket_message",
    ticketId: ticket.id,
    payload: { ticketId: ticket.id, senderType: "ATTENDANT" },
  });

  // Ilha com "Usar assinatura com nome dos atendentes" ligado: toda mensagem
  // de TEXTO do atendente sai como "*Nome:*\n<mensagem>" (negrito do
  // WhatsApp). Mídia não leva assinatura — o texto dela é legenda/nome do
  // arquivo. O Outbound-Worker grava no Mongo exatamente o texto enviado, então
  // o histórico mostra a assinatura como o cliente a viu.
  let outboundText = payload.text;
  const isPlainText = !payload.messageType || payload.messageType === "TEXT";
  if (isPlainText && payload.text.trim() && ticket.queue.serviceIsland.useAttendantSignature) {
    const attendant = await prisma.user.findUnique({ where: { id: payload.attendantUserId }, select: { name: true } });
    // Asterisco no nome quebraria o negrito do WhatsApp.
    const attendantName = attendant?.name.replace(/\*/g, "").trim();
    if (attendantName) outboundText = `*${attendantName}:*\n${payload.text}`;
  }

  console.log(`[DESK-MSG][handleDeskMessageOutbound] ticketId=${payload.ticketId} publicando em outbound.message.send`);

  await publishOutboundMessage(channel, {
    target: ticket.target,
    channel: ticket.target.whatsappChannel,
    messagingSession: ticket.messagingSession,
    answer: { text: outboundText, audio: "", image: "" },
    messageType: payload.messageType,
    mediaUrl: payload.mediaUrl,
    finishesProcessing: true,
    origin: "ATTENDANT",
    ticketId: ticket.id,
  });

  console.log(`[DESK-MSG][handleDeskMessageOutbound] ticketId=${payload.ticketId} concluído — publicado em outbound.message.send`);
}
