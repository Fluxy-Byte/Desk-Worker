import { prisma } from "../../infrastructure/database/prisma/client";

const MESSAGE_LOG = "Desk Worker";

/// Grava uma linha de MessageLog (ver Agent-Api/prisma/schema.prisma pro
/// comentário completo e MENSAGERIA.md pro mapa das filas). Erro aqui nunca
/// pode derrubar o processamento da mensagem — só loga e segue.
export async function recordMessageLog(messageId: string, stagio: "start" | "end"): Promise<void> {
  try {
    await prisma.messageLog.create({ data: { messageId, messageLog: MESSAGE_LOG, stagio } });
  } catch (error) {
    console.error(`[MESSAGE-LOG][message-log-service] messageId=${messageId} stagio=${stagio} — falha ao gravar:`, error);
  }
}
