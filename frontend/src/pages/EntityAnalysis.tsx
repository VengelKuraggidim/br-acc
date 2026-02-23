import { useTranslation } from "react-i18next";
import { useParams } from "react-router";

export function EntityAnalysis() {
  const { t } = useTranslation();
  const { entityId } = useParams<{ entityId: string }>();

  return (
    <div>
      <h1>{t("analysis.title")}</h1>
      <p>{entityId}</p>
    </div>
  );
}
