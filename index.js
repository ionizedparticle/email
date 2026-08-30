import express from 'express';
import Database from 'better-sqlite3';
import multer from 'multer';

const Upload = multer();
const App = express();

App.use(express.json());
App.use(express.urlencoded({ extended: true }));

const Db = new Database('emails.db');

Db.exec(`
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

let SseClients = [];

App.get('/', (Req, Res) => {
  return Res.send('hi');
});

App.get('/api/stream', (Req, Res) => {
  Res.setHeader('Content-Type', 'text/event-stream');
  Res.setHeader('Cache-Control', 'no-cache');
  Res.setHeader('Connection', 'keep-alive');
  Res.flushHeaders();

  const ClientId = Date.now();
  SseClients.push({ id: ClientId, res: Res });

  const KeepAlive = setInterval(() => Res.write(': keepalive\n\n'), 30000);

  Req.on('close', () => {
    clearInterval(KeepAlive);
    SseClients = SseClients.filter(C => C.id !== ClientId);
  });
});

function BroadcastToSSE(Email) {
  console.log('Broadcasting to SSE clients:', SseClients.length);
  const DataString = JSON.stringify(Email);
  SseClients.forEach(C => C.res.write(`data: ${DataString}\n\n`));
}

function ExtractEmailAddress(Input) {
  if (!Input) return '';
  if (typeof Input === 'object') {
    if (Array.isArray(Input.value) && Input.value[0]?.address) {
      return Input.value[0].address.trim();
    }
    if (Input.address) return Input.address.trim();
    if (Input.text) Input = Input.text;
  }
  if (typeof Input === 'string') {
    const Match = Input.match(/<([^>]+)>/);
    return Match ? Match[1].trim() : Input.trim();
  }
  return '';
}

App.post('/api/incoming', Upload.none(), (Req, Res) => {
  console.log('=== INCOMING WEBHOOK RECEIVED ===');
  console.log('Headers:', JSON.stringify(Req.headers, null, 2));
  console.log('Query Params:', JSON.stringify(Req.query, null, 2));
  console.log('Raw Body:', JSON.stringify(Req.body, null, 2));

  const Body = Req.body;

  let Recipient = ExtractEmailAddress(Body.to);
  if (!Recipient && Req.query.inbox) {
    const RawQuery = Req.query.inbox.trim();
    Recipient = RawQuery.includes('@') ? RawQuery : `${RawQuery}@discord.dedyn.io`;
  }
  console.log('Extracted Recipient:', Recipient);

  let Sender = ExtractEmailAddress(Body.from);
  console.log('Extracted Sender:', Sender);

  const EmailPayload = {
    id: Math.random().toString(36).substring(2, 11),
    to: Recipient,
    from: Sender,
    subject: Body.subject || 'No Subject',
    text: Body.text || Body.plain || '',
    html: Body.html || '',
    receivedAt: Date.now()
  };

  console.log('Email Payload to Save:', EmailPayload);

  try {
    const Insert = Db.prepare(`
      INSERT INTO emails (id, inbox_to, mail_from, subject, text_content, html_content, received_at)
      VALUES (@id, @to, @from, @subject, @text, @html, @receivedAt)
    `);
    Insert.run(EmailPayload);
    console.log('Successfully saved to SQLite!');
  } catch (Err) {
    console.error('Database Insertion Error:', Err);
  }

  BroadcastToSSE(EmailPayload);

  return Res.status(200).json({ success: true });
});

App.delete('/api/messages/:id', (Req, Res) => {
  const { id: Id } = Req.params;
  console.log('DELETE request for message ID:', Id);
  const Info = Db.prepare('DELETE FROM emails WHERE id = ?').run(Id);
  
  if (Info.changes === 0) return Res.status(404).json({ error: "Message not found" });
  return Res.json({ success: true, message: `Email ${Id} deleted successfully.` });
});

App.delete('/api/inbox', (Req, Res) => {
  const TargetEmail = Req.query.email;
  console.log('DELETE request for inbox:', TargetEmail);
  if (!TargetEmail) return Res.status(400).json({ error: "Missing 'email' parameter" });

  const Info = Db.prepare('DELETE FROM emails WHERE inbox_to LIKE ?').run(`%${TargetEmail}%`);
  return Res.json({ success: true, deletedCount: Info.changes });
});

App.get('/api/messages', (Req, Res) => {
  const TargetEmail = Req.query.email;
  console.log('GET /api/messages for target email:', TargetEmail);
  if (!TargetEmail) return Res.status(400).json({ error: "Missing 'email' parameter" });

  const Rows = Db.prepare('SELECT * FROM emails WHERE inbox_to LIKE ? ORDER BY received_at DESC').all(`%${TargetEmail}%`);
  console.log('Matched Rows Count:', Rows.length);
  return Res.json({ email: TargetEmail, count: Rows.length, messages: Rows });
});

setInterval(() => {
  const ThirtyDaysAgo = Date.now() - (30 * 24 * 60 * 60 * 1000);
  Db.prepare('DELETE FROM emails WHERE received_at < ?').run(ThirtyDaysAgo);
}, 60000 * 60);

App.listen(3000, '0.0.0.0', () => {
  console.log('Server running on port 3000');
});
