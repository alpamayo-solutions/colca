/** The record types against the vectors of `colca_data_contracts.catalog`. */

import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";

import {
  CATALOG_PRODUCT_FIELDS,
  CATALOG_RECIPE_FIELDS,
  type CatalogProduct,
  type CatalogRecipe,
} from "../src/catalog.js";

interface Vectors {
  products: { valid: CatalogProduct[]; invalid: Record<string, unknown>[] };
  recipes: { valid: CatalogRecipe[]; invalid: Record<string, unknown>[] };
}

const vectors = JSON.parse(
  readFileSync(
    fileURLToPath(
      new URL("../../../contracts/src/colca_data_contracts/vectors/catalog_records.json", import.meta.url),
    ),
    "utf8",
  ),
) as Vectors;

const provenance = ["provenance", "field_provenance"];

describe("catalog records", () => {
  it("name every field a valid record carries", () => {
    const products = new Set<string>([...CATALOG_PRODUCT_FIELDS, ...provenance]);
    const recipes = new Set<string>([...CATALOG_RECIPE_FIELDS, ...provenance]);
    for (const record of vectors.products.valid)
      for (const key of Object.keys(record)) expect(products).toContain(key);
    for (const record of vectors.recipes.valid)
      for (const key of Object.keys(record)) expect(recipes).toContain(key);
  });

  it("keep a hand-entered field's provenance next to the record's", () => {
    const [product] = vectors.products.valid;
    expect(product.provenance?.source).toBe("erp");
    expect(product.field_provenance?.density_kg_m3?.source).toBe("manual");
  });
});
