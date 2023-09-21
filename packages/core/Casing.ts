export type Casing =
  | Casing.camelCase
  | Casing.snake_case
  | Casing.SCREAMING_SNAKE_CASE
  | Casing.PascalCase
  | Casing.TitleCase
  | Casing.kebabcase;

export namespace Casing {
  abstract class Casing {
    convert(input: string): string {
      return this.assemble(
        input
          .split(/(?:(?<=[a-z])(?=[A-Z0-9])|[-_\s]+|(?=[A-Z][a-z]))/g)
          .map((s) => s.trim().toLowerCase())
          .filter(Boolean)
      );
    }

    abstract assemble(words: string[]): string;
  }

  class CamelCaseCasing extends Casing {
    assemble(words: string[]): string {
      return words
        .map((w, i) => (i === 0 ? w : w[0].toUpperCase() + w.slice(1)))
        .join("");
    }
  }

  export const camelCase = new CamelCaseCasing();
  export type camelCase = typeof camelCase;

  class SnakeCaseCasing extends Casing {
    assemble(words: string[]): string {
      return words.join("_");
    }
  }

  export const snake_case = new SnakeCaseCasing();
  export type snake_case = typeof snake_case;

  class ScreamingSnakeCaseCasing extends Casing {
    assemble(words: string[]): string {
      return words.map((w) => w.toUpperCase()).join("_");
    }
  }

  export const SCREAMING_SNAKE_CASE = new ScreamingSnakeCaseCasing();
  export type SCREAMING_SNAKE_CASE = typeof SCREAMING_SNAKE_CASE;

  class PascalCaseCasing extends Casing {
    assemble(words: string[]): string {
      return words.map((w) => w[0].toUpperCase() + w.slice(1)).join("");
    }
  }

  export const PascalCase = new PascalCaseCasing();
  export type PascalCase = typeof PascalCase;

  class TitleCaseCasing extends Casing {
    assemble(words: string[]): string {
      return words.map((w) => w[0].toUpperCase() + w.slice(1)).join(" ");
    }
  }

  export const TitleCase = new TitleCaseCasing();
  export type TitleCase = typeof TitleCase;

  class SentenceCaseCasing extends Casing {
    assemble(words: string[]): string {
      return words
        .map((w, i) => (i === 0 ? w[0].toUpperCase() + w.slice(1) : w))
        .join(" ");
    }
  }

  export const sentenceCase = new SentenceCaseCasing();
  export type sentenceCase = typeof sentenceCase;

  class KebabCaseCasing extends Casing {
    assemble(words: string[]): string {
      return words.join("-");
    }
  }

  export const kebabcase = new KebabCaseCasing();
  export type kebabcase = typeof kebabcase;
}
