import { useState, useCallback } from "react";
import { Heart, X, Copy, Check } from "lucide-react";

import styles from "./PixDonate.module.css";

const PIX_KEY =
  "00020126580014BR.GOV.BCB.PIX013671206eac-72f1-4d7a-8722-bf1451bd1fcc5204000053039865802BR5924Anastacia Almeida Campos6009SAO PAULO621405102QzEyfqeVU6304BF1E";

export function PixDonate() {
  const [open, setOpen] = useState(false);
  const [copied, setCopied] = useState(false);

  const handleCopy = useCallback(async () => {
    try {
      await navigator.clipboard.writeText(PIX_KEY);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    } catch {
      // fallback
      const ta = document.createElement("textarea");
      ta.value = PIX_KEY;
      ta.style.position = "fixed";
      ta.style.opacity = "0";
      document.body.appendChild(ta);
      ta.select();
      document.execCommand("copy");
      document.body.removeChild(ta);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    }
  }, []);

  if (!open) {
    return (
      <button
        className={styles.fab}
        onClick={() => setOpen(true)}
        title="Apoie o projeto via PIX"
        aria-label="Apoie o projeto via PIX"
      >
        <Heart size={18} />
      </button>
    );
  }

  return (
    <div className={styles.panel}>
      <div className={styles.header}>
        <span className={styles.title}>
          <Heart size={14} />
          Apoie o projeto
        </span>
        <button
          className={styles.closeBtn}
          onClick={() => setOpen(false)}
          aria-label="Fechar"
        >
          <X size={14} />
        </button>
      </div>
      <p className={styles.desc}>
        Ajude a manter o BR-ACC no ar com uma contribuicao via PIX.
      </p>
      <div className={styles.pixBox}>
        <code className={styles.pixKey}>{PIX_KEY.slice(0, 30)}...</code>
        <button
          className={styles.copyBtn}
          onClick={handleCopy}
          title={copied ? "Copiado!" : "Copiar chave PIX"}
        >
          {copied ? <Check size={14} /> : <Copy size={14} />}
          {copied ? "Copiado!" : "Copiar"}
        </button>
      </div>
    </div>
  );
}
