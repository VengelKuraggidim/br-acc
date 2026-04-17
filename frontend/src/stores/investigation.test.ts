import {
  afterEach,
  beforeEach,
  describe,
  expect,
  it,
  vi,
} from "vitest";

const {
  listInvestigations,
  createInvestigation: createInvestigationFn,
  deleteInvestigation: deleteInvestigationFn,
  updateInvestigation: updateInvestigationFn,
  addEntityToInvestigation,
  removeEntityFromInvestigation,
  getInvestigation,
  listAnnotations,
  createAnnotation,
  deleteAnnotation: deleteAnnotationFn,
  listTags,
  createTag,
  deleteTag: deleteTagFn,
} = vi.hoisted(() => ({
  listInvestigations: vi.fn(),
  createInvestigation: vi.fn(),
  deleteInvestigation: vi.fn(),
  updateInvestigation: vi.fn(),
  addEntityToInvestigation: vi.fn(),
  removeEntityFromInvestigation: vi.fn(),
  getInvestigation: vi.fn(),
  listAnnotations: vi.fn(),
  createAnnotation: vi.fn(),
  deleteAnnotation: vi.fn(),
  listTags: vi.fn(),
  createTag: vi.fn(),
  deleteTag: vi.fn(),
}));

vi.mock("@/api/client", async () => {
  const actual = await vi.importActual<typeof import("@/api/client")>(
    "@/api/client",
  );
  return {
    ...actual,
    listInvestigations,
    createInvestigation: createInvestigationFn,
    deleteInvestigation: deleteInvestigationFn,
    updateInvestigation: updateInvestigationFn,
    addEntityToInvestigation,
    removeEntityFromInvestigation,
    getInvestigation,
    listAnnotations,
    createAnnotation,
    deleteAnnotation: deleteAnnotationFn,
    listTags,
    createTag,
    deleteTag: deleteTagFn,
  };
});

import { useInvestigationStore } from "./investigation";

function resetStore() {
  useInvestigationStore.setState({
    activeInvestigationId: null,
    investigations: [],
    annotations: [],
    tags: [],
    loading: false,
    error: null,
  });
}

const INV = {
  id: "inv-1",
  title: "Case",
  description: "",
  entity_ids: [],
  created_at: "2026-01-01T00:00:00Z",
  updated_at: "2026-01-01T00:00:00Z",
  share_token: null,
  share_expires_at: null,
};

describe("useInvestigationStore", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    resetStore();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("initial state is clean", () => {
    const s = useInvestigationStore.getState();
    expect(s.activeInvestigationId).toBeNull();
    expect(s.investigations).toEqual([]);
    expect(s.annotations).toEqual([]);
    expect(s.tags).toEqual([]);
    expect(s.loading).toBe(false);
    expect(s.error).toBeNull();
  });

  it("setActiveInvestigation updates id", () => {
    useInvestigationStore.getState().setActiveInvestigation("inv-42");
    expect(useInvestigationStore.getState().activeInvestigationId).toBe(
      "inv-42",
    );
  });

  it("fetchInvestigations success loads list and clears loading", async () => {
    listInvestigations.mockResolvedValueOnce({ investigations: [INV] });
    await useInvestigationStore.getState().fetchInvestigations();
    const s = useInvestigationStore.getState();
    expect(s.investigations).toEqual([INV]);
    expect(s.loading).toBe(false);
    expect(s.error).toBeNull();
  });

  it("fetchInvestigations error sets loadError and clears loading", async () => {
    listInvestigations.mockRejectedValueOnce(new Error("boom"));
    await useInvestigationStore.getState().fetchInvestigations();
    const s = useInvestigationStore.getState();
    expect(s.error).toBe("investigation.loadError");
    expect(s.loading).toBe(false);
  });

  it("createInvestigation prepends the new investigation", async () => {
    useInvestigationStore.setState({ investigations: [INV] });
    const newInv = { ...INV, id: "inv-2", title: "New" };
    createInvestigationFn.mockResolvedValueOnce(newInv);

    const result = await useInvestigationStore
      .getState()
      .createInvestigation("New", undefined);

    expect(result).toEqual(newInv);
    const list = useInvestigationStore.getState().investigations;
    expect(list[0]!.id).toBe("inv-2");
    expect(list[1]!.id).toBe("inv-1");
  });

  it("deleteInvestigation removes from list and clears active if matched", async () => {
    useInvestigationStore.setState({
      investigations: [INV, { ...INV, id: "inv-2" }],
      activeInvestigationId: "inv-1",
    });
    deleteInvestigationFn.mockResolvedValueOnce(undefined);

    await useInvestigationStore.getState().deleteInvestigation("inv-1");

    const s = useInvestigationStore.getState();
    expect(s.investigations.map((i) => i.id)).toEqual(["inv-2"]);
    expect(s.activeInvestigationId).toBeNull();
  });

  it("deleteInvestigation preserves active id when a different one is deleted", async () => {
    useInvestigationStore.setState({
      investigations: [INV, { ...INV, id: "inv-2" }],
      activeInvestigationId: "inv-2",
    });
    deleteInvestigationFn.mockResolvedValueOnce(undefined);

    await useInvestigationStore.getState().deleteInvestigation("inv-1");

    expect(useInvestigationStore.getState().activeInvestigationId).toBe("inv-2");
  });

  it("updateInvestigation replaces entry by id", async () => {
    useInvestigationStore.setState({ investigations: [INV] });
    const updated = { ...INV, title: "Renamed" };
    updateInvestigationFn.mockResolvedValueOnce(updated);

    await useInvestigationStore
      .getState()
      .updateInvestigation("inv-1", { title: "Renamed" });

    expect(useInvestigationStore.getState().investigations[0]!.title).toBe(
      "Renamed",
    );
  });

  it("addEntity fetches fresh investigation and replaces in list", async () => {
    useInvestigationStore.setState({ investigations: [INV] });
    const refreshed = { ...INV, entity_ids: ["ent-1"] };
    addEntityToInvestigation.mockResolvedValueOnce(undefined);
    getInvestigation.mockResolvedValueOnce(refreshed);

    await useInvestigationStore.getState().addEntity("inv-1", "ent-1");

    expect(
      useInvestigationStore.getState().investigations[0]!.entity_ids,
    ).toEqual(["ent-1"]);
  });

  it("removeEntity fetches fresh investigation and replaces in list", async () => {
    useInvestigationStore.setState({
      investigations: [{ ...INV, entity_ids: ["ent-1"] }],
    });
    const refreshed = { ...INV, entity_ids: [] };
    removeEntityFromInvestigation.mockResolvedValueOnce(undefined);
    getInvestigation.mockResolvedValueOnce(refreshed);

    await useInvestigationStore.getState().removeEntity("inv-1", "ent-1");

    expect(
      useInvestigationStore.getState().investigations[0]!.entity_ids,
    ).toEqual([]);
  });

  it("fetchAnnotations success replaces annotations", async () => {
    const ann = {
      id: "a-1",
      entity_id: "e-1",
      investigation_id: "inv-1",
      text: "note",
      created_at: "2026-01-01T00:00:00Z",
    };
    listAnnotations.mockResolvedValueOnce([ann]);
    await useInvestigationStore.getState().fetchAnnotations("inv-1");
    expect(useInvestigationStore.getState().annotations).toEqual([ann]);
  });

  it("fetchAnnotations error resets annotations to empty", async () => {
    useInvestigationStore.setState({
      annotations: [
        {
          id: "old",
          entity_id: "e",
          investigation_id: "i",
          text: "old",
          created_at: "2026",
        },
      ],
    });
    listAnnotations.mockRejectedValueOnce(new Error("boom"));
    await useInvestigationStore.getState().fetchAnnotations("inv-1");
    expect(useInvestigationStore.getState().annotations).toEqual([]);
  });

  it("addAnnotation appends to list", async () => {
    const ann = {
      id: "a-2",
      entity_id: "e-1",
      investigation_id: "inv-1",
      text: "new",
      created_at: "2026-01-01T00:00:00Z",
    };
    createAnnotation.mockResolvedValueOnce(ann);
    await useInvestigationStore
      .getState()
      .addAnnotation("inv-1", "e-1", "new");
    expect(useInvestigationStore.getState().annotations).toEqual([ann]);
  });

  it("deleteAnnotation filters list by id", async () => {
    useInvestigationStore.setState({
      annotations: [
        {
          id: "a-1",
          entity_id: "e",
          investigation_id: "i",
          text: "keep",
          created_at: "2026",
        },
        {
          id: "a-2",
          entity_id: "e",
          investigation_id: "i",
          text: "drop",
          created_at: "2026",
        },
      ],
    });
    deleteAnnotationFn.mockResolvedValueOnce(undefined);
    await useInvestigationStore.getState().deleteAnnotation("inv-1", "a-2");
    expect(
      useInvestigationStore.getState().annotations.map((a) => a.id),
    ).toEqual(["a-1"]);
  });

  it("fetchTags success replaces tags", async () => {
    const tag = { id: "t-1", investigation_id: "inv-1", name: "x", color: "#123456" };
    listTags.mockResolvedValueOnce([tag]);
    await useInvestigationStore.getState().fetchTags("inv-1");
    expect(useInvestigationStore.getState().tags).toEqual([tag]);
  });

  it("fetchTags error resets tags to empty", async () => {
    useInvestigationStore.setState({
      tags: [{ id: "t-old", investigation_id: "i", name: "x", color: "#abcdef" }],
    });
    listTags.mockRejectedValueOnce(new Error("boom"));
    await useInvestigationStore.getState().fetchTags("inv-1");
    expect(useInvestigationStore.getState().tags).toEqual([]);
  });

  it("addTag appends to list", async () => {
    const tag = { id: "t-2", investigation_id: "inv-1", name: "new", color: "#ff0000" };
    createTag.mockResolvedValueOnce(tag);
    await useInvestigationStore.getState().addTag("inv-1", "new", "#ff0000");
    expect(useInvestigationStore.getState().tags).toEqual([tag]);
  });

  it("deleteTag filters list by id", async () => {
    useInvestigationStore.setState({
      tags: [
        { id: "t-1", investigation_id: "i", name: "keep", color: "#111111" },
        { id: "t-2", investigation_id: "i", name: "drop", color: "#222222" },
      ],
    });
    deleteTagFn.mockResolvedValueOnce(undefined);
    await useInvestigationStore.getState().deleteTag("inv-1", "t-2");
    expect(useInvestigationStore.getState().tags.map((t) => t.id)).toEqual([
      "t-1",
    ]);
  });
});
