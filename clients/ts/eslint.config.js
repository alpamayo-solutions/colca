// Type-aware linting: the rules that need the type checker are the ones worth
// having in a client whose whole job is to keep a wire format straight.
import eslint from "@eslint/js";
import tseslint from "typescript-eslint";

export default tseslint.config(
  // src/generated is the bundle's own shape; CI checks it by regenerating it.
  { ignores: ["dist/**", "coverage/**", "src/generated/**"] },
  eslint.configs.recommended,
  tseslint.configs.strictTypeChecked,
  tseslint.configs.stylisticTypeChecked,
  {
    languageOptions: {
      parserOptions: {
        projectService: true,
        tsconfigRootDir: import.meta.dirname,
      },
    },
  },
  {
    // This config and the type generator are plain JavaScript, outside the
    // TypeScript program: no types to check them against, and Node's globals.
    files: ["**/*.js", "**/*.mjs"],
    extends: [tseslint.configs.disableTypeChecked],
    rules: { "no-undef": "off" },
  },
  {
    // The tests hand a hand-written `fetch` to the client and read wire shapes
    // back, so they cast where the sources never would.
    files: ["test/**"],
    rules: {
      "@typescript-eslint/no-unsafe-assignment": "off",
      "@typescript-eslint/no-unsafe-member-access": "off",
    },
  },
);
