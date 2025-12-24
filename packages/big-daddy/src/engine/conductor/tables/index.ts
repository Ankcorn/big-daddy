/**
 * Table operations - CREATE, DROP, ALTER, DESCRIBE, STATS
 */

export { handleAlterTable, handleReshardTable } from "./alter";
export { createTableOperationsAPI, TableOperationsAPI } from "./api";
export { handleCreateTable, handleDropTable } from "./create-drop";
export {
	handleDescribeTable,
	handleShowTables,
	handleTableStats,
} from "./describe";
