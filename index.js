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
  const Body = Req.body;

  let Recipient = ExtractEmailAddress(Body.to);
  if (!Recipient && Req.query.inbox) {
    const RawQuery = Req.query.inbox.trim();
    Recipient = RawQuery.includes('@') ? RawQuery : `${RawQuery}@discord.dedyn.io`;
  }
  let Sender = ExtractEmailAddress(Body.from);

  const EmailPayload = {
    id: Math.random().toString(36).substring(2, 11),
    to: Recipient,
    from: Sender,
    subject: Body.subject || 'No Subject',
    text: Body.text || Body.plain || '',
    html: Body.html || '',
    receivedAt: Date.now()
  };

  try {
    const Insert = Db.prepare(`
      INSERT INTO emails (id, inbox_to, mail_from, subject, text_content, html_content, received_at)
      VALUES (@id, @to, @from, @subject, @text, @html, @receivedAt)
    `);
    Insert.run(EmailPayload);
  } catch (Err) {
    console.error('Database Insertion Error:', Err);
  }

  BroadcastToSSE(EmailPayload);

  return Res.status(200).json({ success: true });
});

App.delete('/api/messages/:id', (Req, Res) => {
  const { id: Id } = Req.params;
  const Info = Db.prepare('DELETE FROM emails WHERE id = ?').run(Id);
  
  if (Info.changes === 0) return Res.status(404).json({ error: "Message not found" });
  return Res.json({ success: true, message: `Email ${Id} deleted successfully.` });
});

App.delete('/api/inbox', (Req, Res) => {
  const TargetEmail = Req.query.email;
  if (!TargetEmail) return Res.status(400).json({ error: "Missing 'email' parameter" });

  const Info = Db.prepare('DELETE FROM emails WHERE inbox_to LIKE ?').run(`%${TargetEmail}%`);
  return Res.json({ success: true, deletedCount: Info.changes });
});

App.get('/api/messages', (Req, Res) => {
  const TargetEmail = Req.query.email;
  if (!TargetEmail) return Res.status(400).json({ error: "Missing 'email' parameter" });

  const Rows = Db.prepare('SELECT * FROM emails WHERE inbox_to LIKE ? ORDER BY received_at DESC').all(`%${TargetEmail}%`);
  return Res.json({ email: TargetEmail, count: Rows.length, messages: Rows });
});

App.post('/api/messages/batch', (Req, Res) => {
  const { emails: TargetEmails } = Req.body;
  if (!Array.isArray(TargetEmails) || TargetEmails.length === 0) {
    return Res.json({ messages: [] });
  }

  const Placeholders = TargetEmails.map(() => '?').join(',');
  
  const Query = `
    SELECT id, inbox_to, mail_from, subject, text_content, html_content, received_at 
    FROM emails 
    WHERE inbox_to IN (${Placeholders}) 
    ORDER BY received_at DESC
  `;

  const Rows = Db.prepare(Query).all(...TargetEmails);
  return Res.json({ messages: Rows });
});

setInterval(() => {
  const ThirtyDaysAgo = Date.now() - (30 * 24 * 60 * 60 * 1000);
  Db.prepare('DELETE FROM emails WHERE received_at < ?').run(ThirtyDaysAgo);
}, 60000 * 60);

App.listen(3000, '0.0.0.0', () => {
  console.log('running');
});
