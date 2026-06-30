// import * as mqtt from 'mqtt';
// import express from 'express';
// import cors from 'cors';
// import { PrismaClient } from '@prisma/client';
// import * as dotenv from 'dotenv';

// dotenv.config();

// const prisma = new PrismaClient();
// const app = express();
// const port = process.env.PORT || 3000;

// // Enable CORS so your frontend framework can fetch data without security errors
// app.use(cors());
// app.use(express.json());

// // ==========================================
// // 1. MQTT INGESTION PIPELINE
// // ==========================================
// const brokerUrl = process.env.MQTT_BROKER_URL;
// const topic = process.env.MQTT_TOPIC || 'muturi/beds/#';

// const mqttOptions: mqtt.IClientOptions = {
//   username: process.env.MQTT_USERNAME,
//   password: process.env.MQTT_PASSWORD,
//   clean: true,
//   connectTimeout: 4000,
//   reconnectPeriod: 1000,
// };

// console.log('--- Initializing Combined Telemetry & API Server ---');
// const mqttClient = mqtt.connect(brokerUrl!, mqttOptions);

// mqttClient.on('connect', () => {
//   console.log('✅ Connected securely to HiveMQ Broker.');
//   mqttClient.subscribe(topic, (err) => {
//     if (!err) console.log(`📡 Subscribed to topic stream: ${topic}`);
//   });
// });

// mqttClient.on('message', async (incomingTopic, payload) => {
//   try {
//     const telemetryData = JSON.parse(payload.toString());
    
//     await prisma.vermicultureTelemetry.create({
//       data: {
//         bedId: telemetryData.bedId || 'Unknown-Bed',
//         moisture: parseFloat(telemetryData.moisture),
//         ph: parseFloat(telemetryData.ph),
//         ambientTemp: parseFloat(telemetryData.ambientTemp),
//         ambientHum: parseFloat(telemetryData.ambientHum),
//         status: telemetryData.status || 'Unknown',
//       },
//     });
//     console.log(`📥 Ingested and stored packet for [${telemetryData.bedId}]`);
//   } catch (error) {
//     console.error('⚠️ MQTT Payload processing failure:', error.message);
//   }
// });

// // ==========================================
// // 2. FRONTEND REST API ENDPOINTS
// // ==========================================

// /**
//  * Endpoint 1: Get Latest Status for All Worm Beds
//  * Use Case: Populates the live overview grid/cards on George's home dashboard.
//  */
// app.get('/api/telemetry/latest', async (req, res) => {
//   try {
//     // Fetches distinct bed IDs
//     const beds = await prisma.vermicultureTelemetry.findMany({
//       select: { bedId: true },
//       distinct: ['bedId'],
//     });

//     // Grabs the single most recent log for each individual bed
//     const latestLogs = await Promise.all(
//       beds.map((bed) =>
//         prisma.vermicultureTelemetry.findFirst({
//           where: { bedId: bed.bedId },
//           orderBy: { recordedAt: 'desc' },
//         })
//       )
//     );

//     return res.status(200).json(latestLogs.filter(Boolean));
//   } catch (error) {
//     return res.status(500).json({ error: 'Failed to retrieve latest status entries.' });
//   }
// });

// /**
//  * Endpoint 2: Get Historical Logs for a Specific Bed (Time-Series)
//  * Use Case: Feeds the chart engine (e.g., ApexCharts/Chart.js) to show trends.
//  * Example URL: /api/telemetry/history?bedId=Bed-001&hours=24
//  */
// app.get('/api/telemetry/history', async (req, res) => {
//   try {
//     const { bedId, hours } = req.query;

//     if (!bedId) {
//       return res.status(400).json({ error: 'Missing required query parameter: bedId' });
//     }

//     // Default to trailing 24 hours if no specific window is passed from frontend
//     const hourWindow = hours ? parseInt(hours as string, 10) : 24;
//     const timeCutoff = new Date();
//     timeCutoff.setHours(timeCutoff.getHours() - hourWindow);

//     const logs = await prisma.vermicultureTelemetry.findMany({
//       where: {
//         bedId: bedId as string,
//         recordedAt: { gte: timeCutoff },
//       },
//       orderBy: { recordedAt: 'asc' }, // Ascending order is mandatory for linear timeline charts
//     });

//     return res.status(200).json(logs);
//   } catch (error) {
//     return res.status(500).json({ error: 'Failed to fetch historical series data.' });
//   }
// });

// // Start the server listener
// app.listen(port, () => {
//   console.log(`🚀 API Gateway active and listening on: http://localhost:${port}`);
// });

// // Graceful Cleanup
// process.on('SIGINT', async () => {
//   mqttClient.end();
//   await prisma.$disconnect();
//   process.exit(0);
// });

import * as mqtt from 'mqtt';
import express, { type Request, type Response, type NextFunction } from 'express';
import cors from 'cors';
import { PrismaClient } from '@prisma/client';
import * as dotenv from 'dotenv';

dotenv.config();

const prisma = new PrismaClient();
const app = express();
const port = process.env.PORT || 3000;

app.use(cors());
app.use(express.json());

// Type definition matching the Prisma model for DTO mapping safety
interface TelemetryLog {
  id: string;
  bedId: string;
  moisture: number;
  ph: number;
  ambientTemp: number;
  ambientHum: number;
  status: string;
  recordedAt: Date;
}

// =========================================================================
// 1. ARCHITECTURAL PATTERNS (DTOs & UNIFIED WRAPPERS)
// =========================================================================

export class TelemetryResponseDto {
  id: string;
  bedId: string;
  moisture: number;
  ph: number;
  ambientTemp: number;
  ambientHum: number;
  status: string;
  recordedAt: Date;

  constructor(model: any) {
    this.id = model.id;
    this.bedId = model.bedId;
    this.moisture = parseFloat(model.moisture.toFixed(1));
    this.ph = parseFloat(model.ph.toFixed(1));
    this.ambientTemp = parseFloat(model.ambientTemp.toFixed(1));
    this.ambientHum = parseFloat(model.ambientHum.toFixed(1));
    this.status = model.status;
    this.recordedAt = model.recordedAt;
  }
}

export class ApiResponse<T> {
  statusCode: number;
  message: string;
  data: T;
  timestamp: string;

  constructor(statusCode: number, message: string, data: T) {
    this.statusCode = statusCode;
    this.message = message;
    this.data = data;
    this.timestamp = new Date().toISOString();
  }
}

export class ErrorResponseDto {
  statusCode: number;
  message: string;
  error: string;
  timestamp: string;

  constructor(statusCode: number, message: string, error: string) {
    this.statusCode = statusCode;
    this.message = message;
    this.error = error;
    this.timestamp = new Date().toISOString();
  }
}

// =========================================================================
// 2. MQTT INGESTION ENGINE
// =========================================================================
const brokerUrl = process.env.MQTT_BROKER_URL;
const topic = process.env.MQTT_TOPIC || 'muturi/beds/bed_001';
// DIAGNOSTIC LOGS: Let's see what the code is actually reading
console.log('\n🔍 [DIAGNOSTIC] Inspecting Loaded Environment Config:');
console.log(`   - Target Broker URL:  "${brokerUrl}"`);
console.log(`   - Target Topic String: "${topic}"`);
console.log(`   - Credentials Status:  ${process.env.MQTT_USERNAME ? '✅ Username Found' : '❌ Username MISSING'}\n`);

const mqttOptions: mqtt.IClientOptions = {
  clean: true,
  connectTimeout: 4000,
  reconnectPeriod: 1000,
};

if (process.env.MQTT_USERNAME) {
  mqttOptions.username = process.env.MQTT_USERNAME;
}
if (process.env.MQTT_PASSWORD) {
  mqttOptions.password = process.env.MQTT_PASSWORD;
}

console.log('--- Initializing Combined Telemetry & API Server ---');
const mqttClient = mqtt.connect(brokerUrl!, mqttOptions);

mqttClient.on('connect', () => {
  console.log('✅ Connected securely to HiveMQ Broker.');
  // mqttClient.subscribe(topic);
  mqttClient.subscribe(topic, (err) => {
    if (err) {
      console.error(`❌ HiveMQ rejected subscription to [${topic}]:`, err.message);
    } else {
      console.log(`✅ Subscription Confirmed! Actively listening for streaming packets...`);
    }
  });
});

mqttClient.on('message', async (incomingTopic, payload) => {
  try {
    const telemetryData = JSON.parse(payload.toString());
    await prisma.vermicultureTelemetry.create({
      data: {
        bedId: telemetryData.bedId || 'Unknown-Bed',
        moisture: parseFloat(telemetryData.moisture),
        ph: parseFloat(telemetryData.ph),
        ambientTemp: parseFloat(telemetryData.ambientTemp),
        ambientHum: parseFloat(telemetryData.ambientHum),
        status: telemetryData.status || 'Unknown',
      },
    });
    console.log("💾 Successfully saved to DB!");
  } catch (error) {
    const err = error as Error;
    console.error('⚠️ MQTT Payload processing failure:', err);
  }
});

// =========================================================================
// 3. REST API ENDPOINTS (WITH ENFORCED RESPONSE DESIGN)
// =========================================================================

app.get('/api/telemetry/latest', async (req: Request, res: Response, next: NextFunction) => {
  try {
    const beds = await prisma.vermicultureTelemetry.findMany({
      select: { bedId: true },
      distinct: ['bedId'],
    });

    const latestLogs = await Promise.all(
      beds.map((bed: { bedId: string }) =>
        prisma.vermicultureTelemetry.findFirst({
          where: { bedId: bed.bedId },
          orderBy: { recordedAt: 'desc' },
        })
      )
    );

    const filteredLogs = latestLogs.filter(Boolean);
    
    // FIX 2: Explicitly typed 'log' to fix implicit any error
    const transformedData = filteredLogs.map((log: TelemetryLog) => new TelemetryResponseDto(log));

    const responseEnvelope = new ApiResponse(200, 'Latest telemetry summary retrieved.', transformedData);
    return res.status(200).json(responseEnvelope);
  } catch (error) {
    next(error); 
  }
});

app.get('/api/telemetry/history', async (req: Request, res: Response, next: NextFunction) => {
  try {
    const { bedId, hours } = req.query;

    if (!bedId) {
      return res.status(400).json(new ErrorResponseDto(400, 'Missing query parameter: bedId', 'BadRequestException'));
    }

    const hourWindow = hours ? parseInt(hours as string, 10) : 24;
    const timeCutoff = new Date();
    timeCutoff.setHours(timeCutoff.getHours() - hourWindow);

    const logs = await prisma.vermicultureTelemetry.findMany({
      where: {
        bedId: bedId as string,
        recordedAt: { gte: timeCutoff },
      },
      orderBy: { recordedAt: 'asc' },
    });

    // FIX 3: Explicitly typed 'log' to fix implicit any error
    const transformedData = logs.map((log: TelemetryLog) => new TelemetryResponseDto(log));
    const responseEnvelope = new ApiResponse(200, `Historical timeline logs for last ${hourWindow} hours.`, transformedData);
    return res.status(200).json(responseEnvelope);
  } catch (error) {
    next(error);
  }
});

app.get('/api/telemetry/recent', async (req: Request, res: Response, next: NextFunction) => {
  try {
    const { bedId, limit } = req.query;

    if (!bedId) {
      return res.status(400).json(new ErrorResponseDto(400, 'Missing query parameter: bedId', 'BadRequestException'));
    }

    const rowLimit = limit ? parseInt(limit as string, 10) : 10;

    const recentLogs = await prisma.vermicultureTelemetry.findMany({
      where: { bedId: bedId as string },
      orderBy: { recordedAt: 'desc' },
      take: rowLimit,
    });

    // FIX 4: Explicitly typed 'log' to fix implicit any error
    const transformedData = recentLogs.map((log: TelemetryLog) => new TelemetryResponseDto(log));
    const responseEnvelope = new ApiResponse(200, `Top ${rowLimit} most recent log records retrieved.`, transformedData);
    return res.status(200).json(responseEnvelope);
  } catch (error) {
    next(error);
  }
});

// =========================================================================
// 4. GLOBAL ERROR EXCEPTION FILTER
// =========================================================================
app.use((err: any, req: Request, res: Response, next: NextFunction) => {
  const status = err.status || 500;
  const message = err.message || 'An unexpected error occurred within the core pipeline.';
  const errorType = err.name || 'InternalServerErrorException';

  console.error(`💥 [Global Error Exception]: ${message}`);

  const errorResponse = new ErrorResponseDto(status, message, errorType);
  return res.status(status).json(errorResponse);
});

app.listen(port, () => {
  console.log(`🚀 Clean API Architecture running on: http://localhost:${port}`);
});
