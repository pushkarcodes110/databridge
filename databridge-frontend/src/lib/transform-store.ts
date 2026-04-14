import { create } from "zustand";

export type TransformMapping = {
  outputColumn: string;
  sourceColumn: string;
};

export type EmailFilterConfig = {
  column: string;
  selectedDomains: string[];
  fixCommonTypos: boolean;
  removeInvalidFormat: boolean;
  verifyMailboxExists: boolean;
  normalizeLowercase: boolean;
  typoRulesExpanded: boolean;
};

export type GenderFilterConfig = {
  nameColumn: string;
  mode: "male" | "female" | "all";
  addGenderColumn: boolean;
};

export type DedupeConfig = {
  removeFullDuplicates: boolean;
  removeDuplicateEmails: boolean;
  emailColumn: string;
  strategy: "first" | "last";
};

export type TransformFilters = {
  email: { enabled: boolean; config: EmailFilterConfig };
  gender: { enabled: boolean; config: GenderFilterConfig };
  deduplication: { enabled: boolean; config: DedupeConfig };
};

type TransformStore = {
  uploadId: string;
  totalRows: number;
  mapping: TransformMapping[];
  filters: TransformFilters;
  setUploadId: (uploadId: string) => void;
  setTotalRows: (totalRows: number) => void;
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
      selectedDomains: [],
      fixCommonTypos: true,
      removeInvalidFormat: true,
      verifyMailboxExists: false,
      normalizeLowercase: true,
      typoRulesExpanded: false,
    },
  },
  gender: {
    enabled: false,
    config: {
      nameColumn: "",
      mode: "all",
      addGenderColumn: true,
    },
  },
  deduplication: {
    enabled: false,
    config: {
      removeFullDuplicates: true,
      removeDuplicateEmails: false,
      emailColumn: "",
      strategy: "first",
    },
  },
};

function cloneDefaultFilters() {
  return structuredClone(defaultFilters);
}

export const useTransformStore = create<TransformStore>((set) => ({
  uploadId: "",
  totalRows: 0,
  mapping: [],
  filters: cloneDefaultFilters(),
  setUploadId: (uploadId) => set({ uploadId }),
  setTotalRows: (totalRows) => set({ totalRows }),
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
    totalRows: 0,
    mapping: [],
    filters: cloneDefaultFilters(),
  }),
}));
