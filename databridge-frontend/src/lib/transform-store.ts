import { create } from "zustand";

export type TransformMapping = {
  outputColumn: string;
  sourceColumn: string;
};

export type EmailFilterConfig = {
  column: string;
  mode: "remove_invalid" | "keep_domain";
  domain: string;
};

export type GenderFilterConfig = {
  column: string;
  allowedValues: string[];
  normalizeValues: boolean;
};

export type DedupeConfig = {
  columns: string[];
  strategy: "first" | "last";
};

export type TransformFilters = {
  email: { enabled: boolean; config: EmailFilterConfig };
  gender: { enabled: boolean; config: GenderFilterConfig };
  deduplication: { enabled: boolean; config: DedupeConfig };
};

type TransformStore = {
  uploadId: string;
  mapping: TransformMapping[];
  filters: TransformFilters;
  setUploadId: (uploadId: string) => void;
  setMapping: (mapping: TransformMapping[]) => void;
  setFilterEnabled: (filter: keyof TransformFilters, enabled: boolean) => void;
  setEmailConfig: (config: Partial<EmailFilterConfig>) => void;
  setGenderConfig: (config: Partial<GenderFilterConfig>) => void;
  setDedupeConfig: (config: Partial<DedupeConfig>) => void;
  resetTransform: () => void;
};

const defaultFilters: TransformFilters = {
  email: {
    enabled: false,
    config: {
      column: "",
      mode: "remove_invalid",
      domain: "",
    },
  },
  gender: {
    enabled: false,
    config: {
      column: "",
      allowedValues: ["female", "male", "non-binary"],
      normalizeValues: true,
    },
  },
  deduplication: {
    enabled: false,
    config: {
      columns: [],
      strategy: "first",
    },
  },
};

function cloneDefaultFilters() {
  return structuredClone(defaultFilters);
}

export const useTransformStore = create<TransformStore>((set) => ({
  uploadId: "",
  mapping: [],
  filters: cloneDefaultFilters(),
  setUploadId: (uploadId) => set({ uploadId }),
  setMapping: (mapping) => set({ mapping }),
  setFilterEnabled: (filter, enabled) => set((state) => ({
    filters: {
      ...state.filters,
      [filter]: {
        ...state.filters[filter],
        enabled,
      },
    },
  })),
  setEmailConfig: (config) => set((state) => ({
    filters: {
      ...state.filters,
      email: {
        ...state.filters.email,
        config: { ...state.filters.email.config, ...config },
      },
    },
  })),
  setGenderConfig: (config) => set((state) => ({
    filters: {
      ...state.filters,
      gender: {
        ...state.filters.gender,
        config: { ...state.filters.gender.config, ...config },
      },
    },
  })),
  setDedupeConfig: (config) => set((state) => ({
    filters: {
      ...state.filters,
      deduplication: {
        ...state.filters.deduplication,
        config: { ...state.filters.deduplication.config, ...config },
      },
    },
  })),
  resetTransform: () => set({
    uploadId: "",
    mapping: [],
    filters: cloneDefaultFilters(),
  }),
}));
