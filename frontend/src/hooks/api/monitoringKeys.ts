export const monitoringKeys = {
  subjects: ["monitoring", "subjects"] as const,
  myDashboard: ["monitoring", "my-dashboard"] as const,
  events: (subjectId: string) => ["monitoring", "subjects", subjectId, "events"] as const,
  power: (subjectId: string, date: string) =>
    ["monitoring", "subjects", subjectId, "power", date] as const,
};
