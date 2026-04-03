/**
 * 附件预览器 — PDF.js / SheetJS / mammoth.js / 图片
 */

const EXT_IMG  = ['jpg','jpeg','png','gif','bmp','webp','tiff','tif'];
const EXT_PDF  = ['pdf'];
const EXT_XLS  = ['xlsx','xls','xlsm'];
const EXT_DOCX = ['docx'];

/**
 * @param {HTMLElement} container 渲染目标容器
 * @param {object}      att       { filename, data_base64, content_type }
 */
export async function renderAttachment(container, att) {
  container.innerHTML = '<div class="empty-hint"><span class="spinner"></span>加载中…</div>';
  const ext = att.filename.split('.').pop().toLowerCase();

  try {
    if (EXT_PDF.includes(ext)) {
      await _renderPdf(container, att.data_base64);
    } else if (EXT_XLS.includes(ext)) {
      _renderExcel(container, att.data_base64);
    } else if (EXT_DOCX.includes(ext)) {
      await _renderDocx(container, att.data_base64);
    } else if (EXT_IMG.includes(ext)) {
      _renderImage(container, att.data_base64, att.content_type, att.filename);
    } else {
      container.innerHTML = `<div class="att-text">此格式不支持预览：${att.filename}</div>`;
    }
  } catch (e) {
    container.innerHTML = `<div class="att-text log-error">预览失败：${e.message}</div>`;
  }
}

// ── base64 → Uint8Array ──────────────────────────────────────

function b64ToUint8(b64) {
  const raw = atob(b64);
  const buf = new Uint8Array(raw.length);
  for (let i = 0; i < raw.length; i++) buf[i] = raw.charCodeAt(i);
  return buf;
}

// ── PDF ──────────────────────────────────────────────────────

async function _renderPdf(container, b64) {
  container.innerHTML = '';
  if (typeof pdfjsLib === 'undefined') {
    container.innerHTML = '<div class="att-text log-error">PDF.js 未加载</div>';
    return;
  }
  pdfjsLib.GlobalWorkerOptions.workerSrc =
    'https://cdnjs.cloudflare.com/ajax/libs/pdf.js/3.11.174/pdf.worker.min.js';

  const data = b64ToUint8(b64);
  const pdf  = await pdfjsLib.getDocument({ data }).promise;

  for (let pageNum = 1; pageNum <= Math.min(pdf.numPages, 20); pageNum++) {
    const page    = await pdf.getPage(pageNum);
    const vp      = page.getViewport({ scale: 1.4 });
    const canvas  = document.createElement('canvas');
    canvas.width  = vp.width;
    canvas.height = vp.height;
    await page.render({ canvasContext: canvas.getContext('2d'), viewport: vp }).promise;
    container.appendChild(canvas);
  }

  if (pdf.numPages > 20) {
    const note = document.createElement('div');
    note.className = 'att-text';
    note.textContent = `（仅显示前 20 页，共 ${pdf.numPages} 页）`;
    container.appendChild(note);
  }
}

// ── Excel ─────────────────────────────────────────────────────

function _renderExcel(container, b64) {
  container.innerHTML = '';
  if (typeof XLSX === 'undefined') {
    container.innerHTML = '<div class="att-text log-error">SheetJS 未加载</div>';
    return;
  }
  const wb = XLSX.read(b64, { type: 'base64' });

  wb.SheetNames.forEach(name => {
    const heading = document.createElement('div');
    heading.style.cssText = 'font-weight:600;padding:4px 0;font-size:12px;color:#374151;';
    heading.textContent = `📊 ${name}`;
    container.appendChild(heading);

    const html = XLSX.utils.sheet_to_html(wb.Sheets[name], { editable: false });
    const div  = document.createElement('div');
    div.style.overflowX = 'auto';
    div.innerHTML = html;
    container.appendChild(div);
  });
}

// ── Word (.docx) ──────────────────────────────────────────────

async function _renderDocx(container, b64) {
  container.innerHTML = '';
  if (typeof mammoth === 'undefined') {
    container.innerHTML = '<div class="att-text log-error">mammoth.js 未加载</div>';
    return;
  }
  const buf    = b64ToUint8(b64).buffer;
  const result = await mammoth.convertToHtml({ arrayBuffer: buf });
  const div    = document.createElement('div');
  div.style.cssText = 'font-size:13px;line-height:1.6;padding:4px;';
  div.innerHTML = result.value;
  container.appendChild(div);
}

// ── 图片 ──────────────────────────────────────────────────────

function _renderImage(container, b64, contentType, filename) {
  container.innerHTML = '';
  const ext = filename.split('.').pop().toLowerCase();
  const mime = contentType || `image/${ext === 'jpg' ? 'jpeg' : ext}`;
  const img = document.createElement('img');
  img.src = `data:${mime};base64,${b64}`;
  img.alt = filename;
  img.style.maxWidth = '100%';
  container.appendChild(img);
}
