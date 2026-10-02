/**
 * Product and recipe records of a node's catalog: the `json` value of the `_Constant`
 * at `catalog/products/{sku}` and at `catalog/recipes/{id}`. The node checks only that
 * the value is JSON; `colca_data_contracts.catalog` holds the rules and the JSON Schema.
 */

/** Who wrote a record or a field: an external system's key, `manual` for a person, `default` for
 * a placeholder. */
export interface CatalogProvenance {
  source: string;
  set_by?: string | null;
  /** RFC 3339. */
  set_at?: string | null;
}

export interface CatalogProduct {
  sku: string;
  name?: string | null;
  substrate?: string | null;
  thickness_mm?: number | null;
  length_mm?: number | null;
  width_mm?: number | null;
  /** What sanding takes off the board, both sides together. */
  sandoff_mm?: number | null;
  density_kg_m3?: number | null;
  /** The grit of the last belt the board sees, FEPA P. */
  final_grit?: number | null;
  /** The recipe, by key: many products share one. */
  recipe_id?: string | null;
  /** The product a line runs when nobody chose one. */
  is_default?: boolean;
  /** Whatever the plant keeps beyond the standard fields. */
  attributes?: Record<string, unknown>;
  provenance?: CatalogProvenance;
  /** Placeholders (`default`) and fields someone other than `provenance.source` set. */
  field_provenance?: Partial<Record<CatalogProductField, CatalogProvenance>>;
}

export interface CatalogRecipe {
  id: string;
  name?: string | null;
  is_default?: boolean;
  /** One grit per head, keyed by the head's path below the line (`mas2/sta1/aggos`);
   * `null` for a head the recipe leaves without a belt. */
  grits?: Record<string, number | null>;
  attributes?: Record<string, unknown>;
  provenance?: CatalogProvenance;
  field_provenance?: Partial<Record<CatalogRecipeField, CatalogProvenance>>;
}

export const CATALOG_PRODUCT_FIELDS = [
  "sku",
  "name",
  "substrate",
  "thickness_mm",
  "length_mm",
  "width_mm",
  "sandoff_mm",
  "density_kg_m3",
  "final_grit",
  "recipe_id",
  "is_default",
  "attributes",
] as const;
export type CatalogProductField = (typeof CATALOG_PRODUCT_FIELDS)[number];

export const CATALOG_RECIPE_FIELDS = ["id", "name", "is_default", "grits", "attributes"] as const;
export type CatalogRecipeField = (typeof CATALOG_RECIPE_FIELDS)[number];

export const CATALOG_PRODUCTS = "catalog/products";
export const CATALOG_RECIPES = "catalog/recipes";
