import { env } from "../../config/env";

interface QueueBusinessHours {
  businessHoursEnabled: boolean;
  businessHoursStart: string | null;
  businessHoursEnd: string | null;
  businessDays: number[];
}

/// Dia da semana + hora atual no fuso da empresa (env.APP_TIMEZONE, default
/// America/Sao_Paulo) — nunca no fuso do processo (containers costumam
/// rodar em UTC). ICU às vezes devolve "24" pra meia-noite com hour12:false;
/// normalizado pra 0 aqui.
function nowInAppTimezone(): { weekday: number; minutesSinceMidnight: number } {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: env.APP_TIMEZONE,
    weekday: "short",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).formatToParts(new Date());

  const WEEKDAY_INDEX: Record<string, number> = { Sun: 0, Mon: 1, Tue: 2, Wed: 3, Thu: 4, Fri: 5, Sat: 6 };
  const weekdayLabel = parts.find((p) => p.type === "weekday")?.value ?? "Sun";
  const hour = Number(parts.find((p) => p.type === "hour")?.value ?? "0") % 24;
  const minute = Number(parts.find((p) => p.type === "minute")?.value ?? "0");

  return { weekday: WEEKDAY_INDEX[weekdayLabel] ?? 0, minutesSinceMidnight: hour * 60 + minute };
}

function toMinutes(hhmm: string): number {
  const [h, m] = hhmm.split(":").map(Number);
  return h * 60 + m;
}

/// Se a fila não tem horário restrito configurado (ou a configuração está
/// incompleta), ela é considerada sempre aberta — só bloqueia quando o
/// horário está de fato ligado e completo.
export function isQueueOpenNow(queue: QueueBusinessHours): boolean {
  if (!queue.businessHoursEnabled) return true;
  if (!queue.businessHoursStart || !queue.businessHoursEnd) return true;

  const { weekday, minutesSinceMidnight } = nowInAppTimezone();
  if (!queue.businessDays.includes(weekday)) return false;

  const start = toMinutes(queue.businessHoursStart);
  const end = toMinutes(queue.businessHoursEnd);

  // Horário que atravessa a meia-noite (ex: 22:00–06:00).
  if (start > end) return minutesSinceMidnight >= start || minutesSinceMidnight < end;

  return minutesSinceMidnight >= start && minutesSinceMidnight < end;
}
