// app.ts
import express, { Request, Response, NextFunction } from 'express';
import cors from 'cors';
import * as dotenv from 'dotenv';
import { identify } from './identify';

dotenv.config();

const app = express();

app.use(cors());
app.use(express.json()); 

// Middleware to log request details for debugging
app.use((req: Request, res: Response, next: NextFunction) => {
  console.log(`[app.ts] Middleware: Incoming request: ${req.method} ${req.url}`);
  console.log('[app.ts] Middleware: Request Headers:', req.headers);
  next();
});

app.post('/identify', (req: Request, res: Response) => {
  console.log('[app.ts] Route handler for /identify reached.');
  console.log('[app.ts] Route handler, req.body (after express.json):', req.body);
  identify(req, res); // Calling the simplified identify function
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Server is running on port ${PORT}`));