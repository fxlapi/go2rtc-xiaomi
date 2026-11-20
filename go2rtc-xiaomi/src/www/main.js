document.head.innerHTML += `
<style>
:root{
    --bg:#0f1115;--bg2:#161a22;--card:#1b2029;--line:#262c38;
    --txt:#e8ebf1;--dim:#8b93a7;--acc:#4f8cff;--acc2:#3a6fd8;
    --ok:#3fbf6f;--warn:#ffb020;--err:#ff5c5c;--radius:12px;
}
*{box-sizing:border-box}

body {
    background: var(--bg);
    color: var(--txt);
    font: 14px/1.5 -apple-system,"PingFang SC","Microsoft YaHei",Roboto,sans-serif;
    display: flex;
    flex-direction: column;
    margin: 0;
    min-height: 100vh;
}

a { color: var(--acc); text-decoration: none; }
a:hover { text-decoration: underline; }

/* navigation block */
header {
    position: sticky;
    top: 0;
    z-index: 20;
    background: rgba(15,17,21,.92);
    backdrop-filter: blur(8px);
    border-bottom: 1px solid var(--line);
    padding-top: env(safe-area-inset-top, 0px);
}

nav {
    display: flex;
    align-items: center;
    gap: 4px;
    height: 48px;
    padding: 0 12px;
    overflow-x: auto;
    scrollbar-width: none;
    -ms-overflow-style: none;
}
nav::-webkit-scrollbar { display: none; }

nav a {
    flex: 0 0 auto;
    white-space: nowrap;
    color: var(--dim);
    padding: 7px 12px;
    border-radius: 8px;
    font-size: 13.5px;
}

nav a b { color: var(--txt); }

nav a:hover {
    color: var(--txt);
    background: var(--bg2);
    text-decoration: none;
}

/* main block */
main {
    flex: 1;
    width: 100%;
    max-width: 1500px;
    margin: 0 auto;
    padding: 14px;
    display: flex;
    flex-direction: column;
    gap: 12px;
}

/* checkbox */
label {
    display: flex;
    gap: 6px;
    align-items: center;
    cursor: pointer;
}

input[type="checkbox"] {
    width: 18px;
    height: 18px;
    cursor: pointer;
    accent-color: var(--acc);
}

input[type="radio"] { accent-color: var(--acc); }

main, main li, main td { overflow-wrap: break-word; }

/* form */
form {
    display: flex;
    flex-wrap: wrap;
    gap: 10px;
    align-items: center;
}

input[type="text"], input[type="email"], input[type="password"], select, textarea {
    font: inherit;
    color: var(--txt);
    background: var(--bg2);
    border: 1px solid var(--line);
    border-radius: 8px;
    padding: 8px 10px;
    outline: none;
}

input:focus, select:focus, textarea:focus { border-color: var(--acc); }

button {
    font: inherit;
    cursor: pointer;
    color: var(--txt);
    background: var(--bg2);
    border: 1px solid var(--line);
    border-radius: 8px;
    padding: 9px 16px;
    font-size: 14px;
}

button:hover { background: var(--card); border-color: var(--acc); }

/* collapsible section buttons (add.html) */
main > button {
    background: var(--card);
    border: 1px solid var(--line);
    text-align: left;
    width: 100%;
    font-weight: 600;
}

/* log levels, tuned for dark theme */
.trace  { color: #6b7280 !important; }
.debug  { color: #9aa3b5 !important; }
.info   { color: var(--acc) !important; }
.warn   { color: var(--warn) !important; }
.error  { color: var(--err) !important; }

/* config editor container */
#config { border-top-color: var(--line) !important; }

/* table */
table {
    width: 100%;
    background: var(--card);
    border-collapse: collapse;
    border-radius: var(--radius);
    overflow: hidden;
}

th, td {
    padding: 11px 14px;
    text-align: left;
    border-bottom: 1px solid var(--line);
    font-size: 13.5px;
}

th {
    background: var(--bg2);
    color: var(--dim);
    font-weight: 500;
}

tr:last-child td { border-bottom: none; }
tr:hover td { background: var(--bg2); }

/* narrow screens */
@media (max-width: 600px) {
    main { padding: 12px 12px 32px; gap: 10px; }
    nav { padding: 0 10px; }
    form { gap: 8px; }
    input[type="text"], input[type="email"], input[type="password"], select { width: 100%; }
    button { flex: 1 1 auto; }

    table, thead, tbody, th, td, tr { display: block; }
    thead { display: none; }
    th, td { width: 100% !important; border: none; padding: 8px 12px; }
    tr {
        margin-bottom: 10px;
        background: var(--card);
        border: 1px solid var(--line);
        border-radius: var(--radius);
        overflow: hidden;
    }
}
</style>
`;

document.body.innerHTML = `
<header>
    <nav>
        <a href="classic.html"><b>go2rtc</b></a>
        <a href="add.html">add</a>
        <a href="config.html">config</a>
        <a href="log.html">log</a>
        <a href="net.html">net</a>
    </nav>
</header>
` + document.body.innerHTML;
