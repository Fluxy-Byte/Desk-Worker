import cors from "cors";
import express from "express";
import { env } from "./config/env";
import { prisma } from "./infrastructure/database/prisma/client";
import { redis } from "./infrastructure/cache/redis/client";
import { getRabbitChannel } from "./infrastructure/queue/rabbitmq/connection";
import { startConsumers } from "./presentation/workers/consumers";

async function main() {
  await prisma.$connect();
  const channel = await getRabbitChannel();
  await startConsumers(channel);

  const app = express();
  app.use(cors());
  app.use(express.json());

  app.get("/health", async (_req, res) => {
    const [dbOk, redisOk] = await Promise.all([
      prisma.$queryRaw`SELECT 1`.then(() => true).catch(() => false),
      redis.ping().then(() => true).catch(() => false),
    ]);

    res.json({ status: "ok", service: "desk-worker", db: dbOk, redis: redisOk });
  });

  app.listen(env.PORT, () => {
    console.log(`Desk-Worker listening on port ${env.PORT}`);
  });
}

main().catch((error) => {
  console.error("Fatal error during Desk-Worker bootstrap:", error);
  process.exit(1);
});
