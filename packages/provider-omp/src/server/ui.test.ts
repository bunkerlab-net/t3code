import { describe, expect, it } from "vite-plus/test";
import { ompApprovalChoices, ompQuestions, ompUiResponse } from "./ui.ts";

describe("native Oh My Pi UI", () => {
  it("passes exact native approval labels and never invents session or permanent grants", () => {
    const event = {
      type: "extension_ui_request",
      id: "approval-1",
      method: "select",
      title: "Allow bash?\nrm build.txt",
      options: ["Approve Once", "Always Approve", "Deny"],
    };
    expect([...ompApprovalChoices(event)]).toEqual([
      ["accept", "Approve Once"],
      ["decline", "Deny"],
    ]);
    expect(ompUiResponse(event, "accept", undefined)).toEqual({ value: "Approve Once" });
    expect(ompUiResponse(event, "decline", undefined)).toEqual({ value: "Deny" });
    expect(ompUiResponse(event, "acceptForSession", undefined)).toBeNull();
    expect(ompUiResponse(event, "acceptAlways", undefined)).toBeNull();
    const withSession = { ...event, options: ["Approve", "Approve bash for this session", "Deny"] };
    expect(ompUiResponse(withSession, "acceptForSession", undefined)).toEqual({
      value: "Approve bash for this session",
    });
  });
  it("keeps select labels including whitespace and declines unknown values", () => {
    const event = { id: "q", method: "select", title: "Choose", options: [" first ", "second"] };
    expect(ompQuestions(event)[0]?.options[0]?.value).toBe(" first ");
    expect(ompUiResponse(event, undefined, { q: " first " })).toEqual({ value: " first " });
    expect(ompUiResponse(event, undefined, { q: "first" })).toBeNull();
    expect(ompUiResponse(event, "cancel", undefined)).toEqual({ cancelled: true });
  });
  it("maps structured ask choices and custom answers without dropping question IDs", () => {
    const event = {
      id: "ask",
      method: "ask",
      questions: [
        { id: "a", question: "Which?", options: [{ label: "One" }, { label: "Two" }], multi: true },
        { id: "b", question: "Why?", options: [] },
      ],
    };
    expect(ompUiResponse(event, undefined, { a: ["One", "Two"], b: "Because" })).toEqual({
      answers: [
        { id: "a", selectedOptions: ["One", "Two"] },
        { id: "b", selectedOptions: [], customInput: "Because" },
      ],
    });
    expect(ompUiResponse(event, undefined, {})).toEqual({ cancelled: true });
  });
});
