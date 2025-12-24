import { describe, expect, it } from "vitest";
import * as index from "./index";

describe("sqlite-ast", () => {
	it("should export types", () => {
		expect(typeof index).toBe("object");
	});

	it("should have a basic test", () => {
		expect(1 + 1).toBe(2);
	});
});
