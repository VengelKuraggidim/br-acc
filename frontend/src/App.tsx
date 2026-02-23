import { lazy, Suspense, useEffect } from "react";
import { Navigate, Route, Routes, useParams } from "react-router";

import { AppShell } from "./components/common/AppShell";
import { PublicShell } from "./components/common/PublicShell";
import { Spinner } from "./components/common/Spinner";
import { Baseline } from "./pages/Baseline";
import { Dashboard } from "./pages/Dashboard";
import { Investigations } from "./pages/Investigations";
import { Landing } from "./pages/Landing";
import { Login } from "./pages/Login";
import { Patterns } from "./pages/Patterns";
import { Register } from "./pages/Register";
import { Search } from "./pages/Search";
import { SharedInvestigation } from "./pages/SharedInvestigation";
import { useAuthStore } from "./stores/auth";

const EntityAnalysis = lazy(() => import("./pages/EntityAnalysis").then((m) => ({ default: m.EntityAnalysis })));

function RequireAuth({ children }: { children: React.ReactNode }) {
  const token = useAuthStore((s) => s.token);
  if (!token) return <Navigate to="/login" replace />;
  return <>{children}</>;
}

function RedirectIfAuth({ children }: { children: React.ReactNode }) {
  const token = useAuthStore((s) => s.token);
  if (token) return <Navigate to="/app" replace />;
  return <>{children}</>;
}

function GraphRedirect() {
  const { entityId } = useParams();
  return <Navigate to={`/app/analysis/${entityId}`} replace />;
}

export function App() {
  const restore = useAuthStore((s) => s.restore);

  useEffect(() => {
    restore();
  }, [restore]);

  return (
    <Routes>
      {/* Public shell — landing, login, register */}
      <Route
        element={
          <RedirectIfAuth>
            <PublicShell />
          </RedirectIfAuth>
        }
      >
        <Route index element={<Landing />} />
        <Route path="login" element={<Login />} />
        <Route path="register" element={<Register />} />
      </Route>

      {/* Public — shared investigation (no auth, no shell) */}
      <Route path="shared/:token" element={<SharedInvestigation />} />

      {/* Authenticated shell — the intelligence workspace */}
      <Route
        path="app"
        element={
          <RequireAuth>
            <AppShell />
          </RequireAuth>
        }
      >
        <Route index element={<Dashboard />} />
        <Route path="search" element={<Search />} />
        <Route path="analysis/:entityId" element={<Suspense fallback={<Spinner />}><EntityAnalysis /></Suspense>} />
        <Route path="graph/:entityId" element={<GraphRedirect />} />
        <Route path="patterns" element={<Patterns />} />
        <Route path="patterns/:entityId" element={<Patterns />} />
        <Route path="baseline/:entityId" element={<Baseline />} />
        <Route path="investigations" element={<Investigations />} />
        <Route path="investigations/:investigationId" element={<Investigations />} />
      </Route>

      {/* Catch-all */}
      <Route path="*" element={<Navigate to="/" replace />} />
    </Routes>
  );
}
