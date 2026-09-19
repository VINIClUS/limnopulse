import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useRef,
  useState,
  type ReactNode,
} from "react";
import { Navigate, useLocation } from "react-router";
import { useQueryClient } from "@tanstack/react-query";
import { currentUser, logout, type SessionUser } from "./auth";
const SessionContext = createContext<{
  user: SessionUser | null;
  loading: boolean;
  refresh: () => Promise<void>;
  exit: () => Promise<void>;
}>({
  user: null,
  loading: true,
  refresh: async () => {},
  exit: async () => {},
});
export function SessionProvider({ children }: { children: ReactNode }) {
  const [user, setUser] = useState<SessionUser | null>(null),
    [loading, setLoading] = useState(true);
  const cache = useQueryClient();
  const generation = useRef(0),
    identity = useRef<SessionUser | null>(null);
  const refresh = useCallback(async () => {
    const ticket = ++generation.current;
    let next: SessionUser | null;
    try {
      next = await currentUser();
    } catch {
      if (ticket === generation.current) setLoading(false);
      return;
    }
    if (ticket !== generation.current) return;
    if (identity.current?.id !== next?.id) {
      await cache.cancelQueries();
      cache.clear();
    }
    if (ticket !== generation.current) return;
    identity.current = next;
    setUser(next);
    setLoading(false);
  }, [cache]);
  const exit = useCallback(async () => {
    ++generation.current;
    identity.current = null;
    setUser(null);
    setLoading(false);
    await cache.cancelQueries();
    cache.clear();
    try {
      await logout();
    } catch {
      /* Local tokens are removed by logout's finally block. */
    }
  }, [cache]);
  useEffect(() => {
    void refresh();
    let timer: ReturnType<typeof setTimeout> | undefined;
    const expired = () => {
      void exit();
    };
    const changed = (event: StorageEvent) => {
      if (
        event.key === null ||
        event.key === "limnopulse:dev-user" ||
        event.key.startsWith("CognitoIdentityServiceProvider.")
      ) {
        // Reconcile the completed SDK write batch before deciding whether the identity changed.
        ++generation.current;
        setLoading(true);
        clearTimeout(timer);
        timer = setTimeout(() => void refresh(), 75);
      }
    };
    window.addEventListener("session-expired", expired);
    window.addEventListener("storage", changed);
    return () => {
      ++generation.current;
      clearTimeout(timer);
      window.removeEventListener("session-expired", expired);
      window.removeEventListener("storage", changed);
    };
  }, [cache, exit, refresh]);
  return (
    <SessionContext.Provider value={{ user, loading, refresh, exit }}>
      {children}
    </SessionContext.Provider>
  );
}
export const useSession = () => useContext(SessionContext);
export function Protected({ children }: { children: ReactNode }) {
  const { user, loading } = useSession();
  const location = useLocation();
  if (loading)
    return (
      <div className="page-loading" role="status">
        Preparando sua operação…
      </div>
    );
  return user ? (
    children
  ) : (
    <Navigate
      to="/entrar"
      replace
      state={{ from: `${location.pathname}${location.search}${location.hash}` }}
    />
  );
}
