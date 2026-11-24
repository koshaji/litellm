import { useTranslation } from "react-i18next";
import { ScopedButton } from "./ControlPlaneShared";

const legacyActions = {
  keys: {
    toolbar: [
      "createServiceAccountKey",
      "bulkMoveKeysToTeam",
      "bulkAssignKeysToUser",
      "bulkSetKeyBudgets",
      "bulkDeleteKeys",
    ],
    row: ["keyDetails", "regenerateKey", "blockKey", "unblockKey", "resetKeySpend", "copyKeyId", "copyKeyAlias"],
  },
  teams: {
    toolbar: ["availableTeams"],
    row: [
      "teamDetails",
      "addTeamMember",
      "bulkAddTeamMembers",
      "editTeamMember",
      "removeTeamMember",
      "teamPermissions",
      "editTeamMemberDefaults",
      "viewTeamMemberKeys",
    ],
  },
  users: {
    toolbar: ["bulkEditUsers"],
    row: ["userDetails", "resetUserPassword", "addUserToTeam", "removeUserFromTeam"],
  },
  models: {
    toolbar: [
      "autoRouters",
      "passThroughEndpoints",
      "modelHealth",
      "modelRetrySettings",
      "modelGroupAliases",
      "accessGroupBudgets",
      "priceData",
      "refreshModels",
    ],
    row: ["modelDetails", "testModel", "modelHealthCheck", "copyModelName"],
  },
  budgets: { toolbar: ["budgetFilters"], row: [] },
  organizations: {
    toolbar: ["organizationFilters"],
    row: ["organizationDetails", "addOrganizationMember", "editOrganizationMember", "removeOrganizationMember"],
  },
  guardrails: {
    toolbar: [
      "guardrailGarden",
      "createCustomGuardrail",
      "guardrailPlayground",
      "guardrailSubmissions",
      "teamGuardrails",
    ],
    row: ["guardrailDetails", "editGuardrailCode", "testGuardrail", "approveGuardrail", "rejectGuardrail"],
  },
  mcp: {
    toolbar: ["importMcpServers", "discoverMcpServers", "mcpSubmissions", "mcpToolsets", "mcpStandards"],
    row: [
      "mcpServerDetails",
      "connectMcpServer",
      "testMcpTool",
      "mcpPermissions",
      "mcpCosts",
      "mcpUserCredentials",
      "refreshMcpUserCredentials",
      "revokeMcpUserCredential",
      "approveMcpServer",
      "rejectMcpServer",
    ],
  },
  credentials: {
    toolbar: ["userConnections"],
    row: ["credentialDetails", "connectCredential", "disconnectCredential"],
  },
} as const;

export function ControlPlaneLegacyActions({
  section,
  placement,
}: {
  section: keyof typeof legacyActions;
  placement: "toolbar" | "row";
}) {
  const { t } = useTranslation("translation", { keyPrefix: "controlPlane" });
  return legacyActions[section][placement].map((action) => (
    <ScopedButton key={action} reason={t("managementWriteUnavailable")}>
      {t(`legacyActions.${action}`)}
    </ScopedButton>
  ));
}
