import { redis } from "./client";

/// TTL de segurança — se o atendente nunca responder, a chave não fica presa
/// pra sempre (o "digitando..." só faz sentido pra resposta em cima da hora).
const TTL_SECONDS = 600;

function key(messagingSessionId: string): string {
  return `desk:last-inbound:${messagingSessionId}`;
}

/// Guarda o externalMessageId da última mensagem do cliente numa sessão —
/// gravado em handleDeskMessageInbound, consumido em
/// handleDeskMessageOutbound pra marcar leitura + "digitando..." assim que o
/// atendente responde.
export async function setLastInboundMessage(messagingSessionId: string, externalMessageId: string): Promise<void> {
  await redis.set(key(messagingSessionId), externalMessageId, "EX", TTL_SECONDS);
}

export async function getLastInboundMessage(messagingSessionId: string): Promise<string | null> {
  return redis.get(key(messagingSessionId));
}
