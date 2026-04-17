import { lazy, Suspense, useEffect } from "react";
import { Navigate, Route, Routes, useParams } from "react-router";

import { AppShell } from "./components/common/AppShell";
import { PublicShell } from "./components/common/PublicShell";
import { Spinner } from "./components/common/Spinner";
import { IS_PATTERNS_ENABLED, IS_PUBLIC_MODE } from "./config/runtime";
import { Dashboard } from "./pages/Dashboard";
import { Landing } from "./pages/Landing";
import { Search } from "./pages/Search";
import { useAuthStore } from "./stores/auth";

const Login = lazy(() => import("./pages/Login").then((m) => ({ default: m.Login })));
const Register = lazy(() => import("./pages/Register").then((m) => ({ default: m.Register })));
const EntityAnalysis = lazy(() =>
  import("./pages/EntityAnalysis").then((m) => ({ default: m.EntityAnalysis })),
);
const Emendas = lazy(() => import("./pages/Emendas").then((m) => ({ default: m.Emendas })));
const Patterns = lazy(() => import("./pages/Patterns").then((m) => ({ default: m.Patterns })));
const Investigations = lazy(() =>
  import("./pages/Investigations").then((m) => ({ default: m.Investigations })),
);
const Baseline = lazy(() => import("./pages/Baseline").then((m) => ({ default: m.Baseline })));
const SharedInvestigation = lazy(() =>
  import("./pages/SharedInvestigation").then((m) => ({ default: m.SharedInvestigation })),
);

function RequireAuth({ children }: { children: React.ReactNode }) {
  const token = useAuthStore((s) => s.token);
  const restored = useAuthStore((s) => s.restored);
  if (!restored) return <Spinner />;
  if (!token) return <Navigate to="/login" replace />;
  return <>{children}</>;
}

function RedirectIfAuth({ children }: { children: React.ReactNode }) {
  const token = useAuthStore((s) => s.token);
  const restored = useAuthStore((s) => s.restored);
  if (!restored) return <Spinner />;
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
        element={IS_PUBLIC_MODE ? <PublicShell /> : (
          <RedirectIfAuth>
            <PublicShell />
          </RedirectIfAuth>
        )}
      >
        <Route index element={<Landing />} />
        {!IS_PUBLIC_MODE && (
          <Route
            path="login"
            element={<Suspense fallback={<Spinner />}><Login /></Suspense>}
          />
        )}
        {!IS_PUBLIC_MODE && (
          <Route
            path="register"
            element={<Suspense fallback={<Spinner />}><Register /></Suspense>}
          />
        )}
      </Route>

      {/* Public — shared investigation (no auth, no shell) */}
      <Route
        path="shared/:token"
        element={
          <Suspense fallback={<Spinner />}>
            <SharedInvestigation />
          </Suspense>
        }
      />

      {/* Authenticated shell — the intelligence workspace */}
      <Route
        path="app"
        element={IS_PUBLIC_MODE ? <AppShell /> : (
          <RequireAuth>
            <AppShell />
          </RequireAuth>
        )}
      >
        <Route index element={<Dashboard />} />
        <Route path="search" element={<Search />} />
        <Route path="analysis/:entityId" element={<Suspense fallback={<Spinner />}><EntityAnalysis /></Suspense>} />
        <Route path="emendas" element={<Suspense fallback={<Spinner />}><Emendas /></Suspense>} />
        <Route path="graph/:entityId" element={<GraphRedirect />} />
        {IS_PATTERNS_ENABLED && (
          <Route
            path="patterns"
            element={<Suspense fallback={<Spinner />}><Patterns /></Suspense>}
          />
        )}
        {IS_PATTERNS_ENABLED && (
          <Route
            path="patterns/:entityId"
            element={<Suspense fallback={<Spinner />}><Patterns /></Suspense>}
          />
        )}
        <Route
          path="baseline/:entityId"
          element={<Suspense fallback={<Spinner />}><Baseline /></Suspense>}
        />
        {!IS_PUBLIC_MODE && (
          <Route
            path="investigations"
            element={<Suspense fallback={<Spinner />}><Investigations /></Suspense>}
          />
        )}
        {!IS_PUBLIC_MODE && (
          <Route
            path="investigations/:investigationId"
            element={<Suspense fallback={<Spinner />}><Investigations /></Suspense>}
          />
        )}
      </Route>

      {/* Catch-all */}
      <Route path="*" element={<Navigate to="/" replace />} />
    </Routes>
  );
}
