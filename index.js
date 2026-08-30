import express from 'express';
import { SMTPServer } from 'smtp-server';
import { simpleParser } from 'mailparser';
import Database from 'better-sqlite3';

const app = express();
app.use(express.json());

const db = new Database('emails.db');

db.exec(`
  CREATE TABLE IF NOT EXISTS emails (
    id TEXT PRIMARY KEY,
    inbox_to TEXT,
    mail_from TEXT,
    subject TEXT,
    text_content TEXT,
    html_content TEXT,
    received_at INTEGER
  );
  CREATE INDEX IF NOT EXISTS idx_inbox ON emails(inbox_to);
`);

let sseClients = [];

const smtpServer = new SMTPServer({
  disabledCommands: ['AUTH'],
  authOptional: true,
  
  onData(stream, session, callback) {
    simpleParser(stream)
      .then(parsed => {
        const cleanEmail = {
          id: Math.random().toString(36).substring(2, 11),
          to: parsed.to?.text || '',
          from: parsed.from?.text || '',
          subject: parsed.subject || 'No Subject',
          text: parsed.text || '',
          html: parsed.html || '',
          receivedAt: Date.now()
        };

        const insert = db.prepare(`
          INSERT INTO emails (id, inbox_to, mail_from, subject, text_content, html_content, received_at)
          VALUES (@id, @to, @from, @subject, @text, @html, @receivedAt)
        `);
        insert.run(cleanEmail);

        broadcastToSSE(cleanEmail);
      })
      .catch(() => {})
      .finally(() => callback());
  }
});

smtpServer.listen(25, '0.0.0.0');


app.get('/api/stream', (req, res) => {
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders();

  const clientId = Date.now();
  sseClients.push({ id: clientId, res });

  const keepAlive = setInterval(() => res.write(': keepalive\n\n'), 30000);

  req.on('close', () => {
    clearInterval(keepAlive);
    sseClients = sseClients.filter(c => c.id !== clientId);
  });
});

function broadcastToSSE(email) {
  const dataString = JSON.stringify(email);
  sseClients.forEach(c => c.res.write(`data: ${dataString}\n\n`));
}

app.post('/api/incoming', (req, res) => {
  const { envelope, headers, plain, html } = req.body;

  const emailPayload = {
    id: Math.random().toString(36).substring(2, 11),
    to: envelope?.to || '',
    from: envelope?.from || '',
    subject: headers?.subject || 'No Subject',
    text: plain || '',
    html: html || '',
    receivedAt: Date.now()
  };

  const insert = db.prepare(`
    INSERT INTO emails (id, inbox_to, mail_from, subject, text_content, html_content, received_at)
    VALUES (@id, @to, @from, @subject, @text, @html, @receivedAt)
  `);

  insert.run(emailPayload);
  broadcastToSSE(emailPayload);

  return res.status(200).json({ success: true });
});

app.delete('/api/messages/:id', (req, res) => {
  const { id } = req.params;
  const info = db.prepare('DELETE FROM emails WHERE id = ?').run(id);
  
  if (info.changes === 0) return res.status(404).json({ error: "Message not found" });
  return res.json({ success: true, message: `Email ${id} deleted successfully.` });
});

app.delete('/api/inbox', (req, res) => {
  const targetEmail = req.query.email;
  if (!targetEmail) return res.status(400).json({ error: "Missing 'email' parameter" });

  const info = db.prepare('DELETE FROM emails WHERE inbox_to LIKE ?').run(`%${targetEmail}%`);
  return res.json({ success: true, deletedCount: info.changes });
});

app.get('/api/messages', (req, res) => {
  const targetEmail = req.query.email;
  if (!targetEmail) return res.status(400).json({ error: "Missing 'email' parameter" });

  const rows = db.prepare('SELECT * FROM emails WHERE inbox_to LIKE ? ORDER BY received_at DESC').all(`%${targetEmail}%`);
  return res.json({ email: targetEmail, count: rows.length, messages: rows });
});

setInterval(() => {
  const thirtyDaysAgo = Date.now() - (30 * 24 * 60 * 60 * 1000);
  db.prepare('DELETE FROM emails WHERE received_at < ?').run(thirtyDaysAgo);
}, 60000 * 60);


app.listen(3000, '0.0.0.0');
