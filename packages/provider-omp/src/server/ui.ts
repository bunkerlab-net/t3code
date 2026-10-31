import type {
  OrchestrationV2UserInputQuestion,
  ProviderApprovalDecision,
} from "@t3tools/contracts";
import { ompRecordField as field, ompRecordString as string, type OmpRpcRecord } from "./rpc.ts";

function labels(value: unknown): string[] {
  return Array.isArray(value)
    ? value.filter((entry): entry is string => typeof entry === "string" && entry.trim().length > 0)
    : [];
}

export function ompApprovalChoices(event: OmpRpcRecord): Map<ProviderApprovalDecision, string> {
  if (event.method === "confirm")
    return new Map([
      ["accept", "Approve"],
      ["decline", "Deny"],
    ]);
  const options = labels(event.options);
  const once = options.includes("Approve Once")
    ? "Approve Once"
    : options.includes("Approve")
      ? "Approve"
      : undefined;
  if (event.method !== "select" || once === undefined || !options.includes("Deny"))
    return new Map();
  const choices = new Map<ProviderApprovalDecision, string>([
    ["accept", once],
    ["decline", "Deny"],
  ]);
  const session = options.find((option) => /^Approve .+ for this session$/.test(option));
  if (session !== undefined) choices.set("acceptForSession", session);
  return choices;
}

export function ompQuestions(event: OmpRpcRecord): ReadonlyArray<OrchestrationV2UserInputQuestion> {
  if (event.method === "ask" && Array.isArray(event.questions)) {
    return event.questions.flatMap((question) => {
      const id = string(question, "id");
      const text = string(question, "question");
      if (id === undefined || text === undefined || !text.trim()) return [];
      const options = field(question, "options");
      return [
        {
          id,
          header: string(question, "header") || "Question",
          question: text,
          options: (Array.isArray(options) ? options : []).flatMap((option) => {
            const label = string(option, "label");
            if (label === undefined || !label.trim()) return [];
            return [
              {
                label,
                value: label,
                description: string(option, "description") || label,
                ...(string(option, "preview") === undefined
                  ? {}
                  : { preview: string(option, "preview")! }),
              },
            ];
          }),
          multiSelect: field(question, "multi") === true,
          allowCustomAnswer: true,
        },
      ];
    });
  }
  const id = string(event, "id") ?? "question";
  const title = string(event, "title") || "Oh My Pi question";
  return [
    {
      id,
      header: "Oh My Pi",
      question: title,
      options: labels(event.options).map((label) => ({ label, value: label, description: label })),
      allowCustomAnswer: event.method !== "select",
      ...(string(event, "prefill") === undefined
        ? {}
        : { initialAnswer: string(event, "prefill")! }),
    },
  ];
}

function answerValues(answer: unknown): ReadonlyArray<string> {
  if (typeof answer === "string") return [answer];
  if (Array.isArray(answer)) return labels(answer);
  const values = field(answer, "selectedOptions");
  return labels(values);
}

export function ompUiResponse(
  event: OmpRpcRecord,
  decision: ProviderApprovalDecision | undefined,
  answers: Record<string, unknown> | undefined,
): OmpRpcRecord | null {
  if (decision === "cancel") return { cancelled: true };
  const choices = ompApprovalChoices(event);
  if (choices.size > 0) {
    if (decision === undefined || !choices.has(decision)) return null;
    return event.method === "confirm"
      ? { confirmed: decision === "accept" }
      : { value: choices.get(decision)! };
  }
  if (event.method === "ask") {
    const questions = ompQuestions(event);
    const response = questions.map((question) => {
      const answer = answers?.[question.id];
      const values = answerValues(answer);
      const options = new Set(question.options.map((option) => option.value ?? option.label));
      const selectedOptions = values.filter((value) => options.has(value));
      const customInput = values.filter((value) => !options.has(value)).join("\n");
      return { id: question.id, selectedOptions, ...(customInput ? { customInput } : {}) };
    });
    return response.length &&
      response.every((answer) => answer.selectedOptions.length || answer.customInput)
      ? { answers: response }
      : { cancelled: true };
  }
  const id = string(event, "id");
  const answer = id === undefined ? undefined : answers?.[id];
  if (typeof answer !== "string") return { cancelled: true };
  if (event.method === "select" && !labels(event.options).includes(answer)) return null;
  return { value: answer };
}
