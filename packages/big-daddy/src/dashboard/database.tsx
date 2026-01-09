import type {
	AsyncJob,
	StorageNode,
	TableMetadata,
	TableShard,
	TopologyData,
	VirtualIndex,
} from "../engine/topology/types";

/**
 * Dashboard page component - displays topology for a specific database
 */
export const DashboardPage = ({
	databaseId,
	topology,
	sqlResult,
	sqlError,
	lastQuery,
}: {
	databaseId: string;
	topology: TopologyData;
	sqlResult?: unknown;
	sqlError?: string;
	lastQuery?: string;
}) => (
	<>
		<title>Big Daddy - {databaseId}</title>

		<div class="py-8 border-b-2 border-black mb-8">
			<h1 class="text-3xl font-bold text-black">Big Daddy</h1>
			<p class="mt-2 text-sm text-black font-mono">Database: {databaseId}</p>
		</div>

		{/* SQL Execution Form */}
		<div class="bg-white border-2 border-black p-6 mb-8">
			<h2 class="text-sm font-semibold text-black uppercase tracking-wide mb-4">
				Execute SQL
			</h2>
			<form method="post" action={`/dash/${databaseId}/sql`} class="space-y-3">
				<textarea
					id="sql-query"
					name="query"
					placeholder="Enter SQL query..."
					class="w-full px-3 py-2 border-2 border-black text-sm font-mono focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-black"
					rows={3}
				>
					{lastQuery}
				</textarea>
				<div class="flex gap-3">
					<button
						type="submit"
						class="flex-1 bg-black text-white font-medium py-2 border-2 border-black hover:bg-white hover:text-black transition"
					>
						Execute
					</button>
					<button
						type="button"
						onclick="document.getElementById('sql-query').value = ''"
						class="flex-1 bg-white text-black font-medium py-2 border-2 border-black hover:bg-black hover:text-white transition"
					>
						Clear
					</button>
				</div>
			</form>

			{sqlError && (
				<div class="mt-4 border-2 border-black bg-white p-4">
					<div class="text-sm font-semibold text-black mb-2">Error:</div>
					<div class="text-xs font-mono text-black whitespace-pre-wrap break-words max-h-96 overflow-y-auto">
						{sqlError}
					</div>
				</div>
			)}

			{sqlResult && !sqlError && (
				<div class="mt-4 border-2 border-black bg-white p-4">
					<div class="text-sm font-semibold text-black mb-2">Result:</div>
					<div class="text-xs font-mono text-black whitespace-pre-wrap break-words max-h-96 overflow-y-auto">
						{JSON.stringify(sqlResult, null, 2)}
					</div>
				</div>
			)}
		</div>

		<div>
			<div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6 mb-8">
				{/* Topology Overview */}
				<div class="bg-white border-2 border-black p-6">
					<h2 class="text-sm font-semibold text-black uppercase tracking-wide mb-4">
						Topology Overview
					</h2>
					<div class="space-y-3">
						<div class="flex justify-between items-center text-sm border-b border-black pb-2">
							<span class="text-black">Storage Nodes</span>
							<span class="font-semibold text-black">
								{topology.storage_nodes.length}
							</span>
						</div>
						<div class="flex justify-between items-center text-sm border-b border-black pb-2">
							<span class="text-black">Tables</span>
							<span class="font-semibold text-black">
								{topology.tables.length}
							</span>
						</div>
						<div class="flex justify-between items-center text-sm border-b border-black pb-2">
							<span class="text-black">Virtual Indexes</span>
							<span class="font-semibold text-black">
								{topology.virtual_indexes.length}
							</span>
						</div>
						<div class="flex justify-between items-center text-sm border-b border-black pb-2">
							<span class="text-black">Index Entries</span>
							<span class="font-semibold text-black">
								{topology.virtual_index_entries.length}
							</span>
						</div>
						<div class="flex justify-between items-center text-sm pt-2">
							<span class="text-black">Async Jobs</span>
							<span class="font-semibold text-black">
								{topology.async_jobs?.length || 0}
							</span>
						</div>
					</div>
				</div>

				{/* Quick Stats */}
				<div class="bg-white border-2 border-black p-6">
					<h2 class="text-sm font-semibold text-black uppercase tracking-wide mb-4">
						Quick Stats
					</h2>
					<div class="space-y-3">
						<div class="flex justify-between items-center text-sm border-b border-black pb-2">
							<span class="text-black">Total Table Shards</span>
							<span class="font-semibold text-black">
								{topology.table_shards.length}
							</span>
						</div>
						<div class="flex justify-between items-center text-sm border-b border-black pb-2">
							<span class="text-black">Avg Node Capacity</span>
							<span class="font-semibold text-black">
								{(
									topology.storage_nodes.reduce(
										(sum: number, n: StorageNode) => sum + n.capacity_used,
										0,
									) / topology.storage_nodes.length
								).toFixed(1)}
								%
							</span>
						</div>
						<div class="flex justify-between items-center text-sm border-b border-black pb-2">
							<span class="text-black">Healthy Nodes</span>
							<span class="font-semibold text-black">
								{
									topology.storage_nodes.filter(
										(n: StorageNode) => n.status === "active",
									).length
								}
								/{topology.storage_nodes.length}
							</span>
						</div>
						<div class="flex justify-between items-center text-sm pt-2">
							<span class="text-black">Running Jobs</span>
							<span class="font-semibold text-black">
								{topology.async_jobs?.filter(
									(j: AsyncJob) => j.status === "running",
								).length || 0}
							</span>
						</div>
					</div>
				</div>
			</div>

			{/* Storage Nodes Section */}
			<div class="bg-white border-2 border-black p-6 mb-8">
				<h2 class="text-sm font-semibold text-black uppercase tracking-wide mb-4">
					Storage Nodes
				</h2>
				{topology.storage_nodes.length === 0 ? (
					<p class="text-center text-black text-sm py-8">No storage nodes</p>
				) : (
					<div class="overflow-x-auto">
						<table class="w-full text-sm border-collapse">
							<thead>
								<tr class="border-b-2 border-black">
									<th class="text-left px-4 py-3 font-semibold text-black">
										Node ID
									</th>
									<th class="text-left px-4 py-3 font-semibold text-black">
										Status
									</th>
									<th class="text-left px-4 py-3 font-semibold text-black">
										Capacity Used
									</th>
									<th class="text-left px-4 py-3 font-semibold text-black">
										Error
									</th>
								</tr>
							</thead>
							<tbody>
								{topology.storage_nodes.map((node: StorageNode) => (
									<tr key={node.node_id} class="border-b border-black">
										<td class="px-4 py-3 font-mono text-xs text-black">
											{node.node_id.substring(0, 8)}...
										</td>
										<td class="px-4 py-3">
											<span
												class={`inline-block px-2 py-1 text-xs font-medium badge badge-${node.status}`}
											>
												{node.status}
											</span>
										</td>
										<td class="px-4 py-3 text-black">{node.capacity_used}%</td>
										<td class="px-4 py-3 text-black">{node.error || "-"}</td>
									</tr>
								))}
							</tbody>
						</table>
					</div>
				)}
			</div>

			{/* Tables Section */}
			<div class="bg-white border-2 border-black p-6 mb-8">
				<h2 class="text-sm font-semibold text-black uppercase tracking-wide mb-4">
					Tables
				</h2>
				{topology.tables.length === 0 ? (
					<p class="text-center text-black text-sm py-8">No tables</p>
				) : (
					<div class="overflow-x-auto">
						<table class="w-full text-sm border-collapse">
							<thead>
								<tr class="border-b-2 border-black">
									<th class="text-left px-4 py-3 font-semibold text-black">
										Table Name
									</th>
									<th class="text-left px-4 py-3 font-semibold text-black">
										Primary Key
									</th>
									<th class="text-left px-4 py-3 font-semibold text-black">
										Shard Key
									</th>
									<th class="text-left px-4 py-3 font-semibold text-black">
										Shards
									</th>
								</tr>
							</thead>
							<tbody>
								{topology.tables.map((table: TableMetadata) => (
									<tr key={table.table_name} class="border-b border-black">
										<td class="px-4 py-3 text-black">{table.table_name}</td>
										<td class="px-4 py-3 text-black">{table.primary_key}</td>
										<td class="px-4 py-3 text-black">{table.shard_key}</td>
										<td class="px-4 py-3 text-black">{table.num_shards}</td>
									</tr>
								))}
							</tbody>
						</table>
					</div>
				)}
			</div>

			{/* Virtual Indexes Section */}
			<div class="bg-white border-2 border-black p-6">
				<h2 class="text-sm font-semibold text-black uppercase tracking-wide mb-4">
					Virtual Indexes
				</h2>
				{topology.virtual_indexes.length === 0 ? (
					<p class="text-center text-black text-sm py-8">No indexes</p>
				) : (
					<div class="overflow-x-auto">
						<table class="w-full text-sm border-collapse">
							<thead>
								<tr class="border-b-2 border-black">
									<th class="text-left px-4 py-3 font-semibold text-black">
										Index Name
									</th>
									<th class="text-left px-4 py-3 font-semibold text-black">
										Table
									</th>
									<th class="text-left px-4 py-3 font-semibold text-black">
										Columns
									</th>
									<th class="text-left px-4 py-3 font-semibold text-black">
										Type
									</th>
									<th class="text-left px-4 py-3 font-semibold text-black">
										Status
									</th>
								</tr>
							</thead>
							<tbody>
								{topology.virtual_indexes.map((index: VirtualIndex) => (
									<tr key={index.index_name} class="border-b border-black">
										<td class="px-4 py-3 text-black">{index.index_name}</td>
										<td class="px-4 py-3 text-black">{index.table_name}</td>
										<td class="px-4 py-3 text-black">
											{JSON.parse(index.columns).join(", ")}
										</td>
										<td class="px-4 py-3 text-black">{index.index_type}</td>
										<td class="px-4 py-3">
											<span
												class={`inline-block px-2 py-1 text-xs font-medium badge badge-${index.status}`}
											>
												{index.status}
											</span>
										</td>
									</tr>
								))}
							</tbody>
						</table>
					</div>
				)}
			</div>

			{/* Virtual Shards Section */}
			<div class="bg-white border-2 border-black p-6 mb-8">
				<h2 class="text-sm font-semibold text-black uppercase tracking-wide mb-4">
					Virtual Shards
				</h2>
				{topology.table_shards.length === 0 ? (
					<p class="text-center text-black text-sm py-8">No virtual shards</p>
				) : (
					<div class="overflow-x-auto">
						<table class="w-full text-sm border-collapse">
							<thead>
								<tr class="border-b-2 border-black">
									<th class="text-left px-4 py-3 font-semibold text-black">
										Shard ID
									</th>
									<th class="text-left px-4 py-3 font-semibold text-black">
										Table
									</th>
									<th class="text-left px-4 py-3 font-semibold text-black">
										Shard Key
									</th>
									<th class="text-left px-4 py-3 font-semibold text-black">
										Storage Node
									</th>
									<th class="text-left px-4 py-3 font-semibold text-black">
										Status
									</th>
									<th class="text-center px-4 py-3 font-semibold text-black">
										Delete
									</th>
								</tr>
							</thead>
							<tbody>
								{topology.table_shards.map((shard: TableShard) => {
									const table = topology.tables.find(
										(t: TableMetadata) => t.table_name === shard.table_name,
									);
									const canDelete = [
										"to_be_deleted",
										"pending",
										"failed",
									].includes(shard.status);
									return (
										<tr
											key={`${shard.table_name}-${shard.shard_id}`}
											class="border-b border-black"
										>
											<td class="px-4 py-3 font-mono text-xs text-black">
												{shard.shard_id}
											</td>
											<td class="px-4 py-3 text-black">{shard.table_name}</td>
											<td class="px-4 py-3 text-black">
												{table?.shard_key || "-"}
											</td>
											<td class="px-4 py-3 font-mono text-xs text-black">
												{shard.node_id?.substring(0, 8) || "-"}...
											</td>
											<td class="px-4 py-3">
												<span
													class={`inline-block px-2 py-1 text-xs font-medium badge badge-${shard.status}`}
												>
													{shard.status}
												</span>
											</td>
											<td class="px-4 py-3 text-center">
												{canDelete && (
													<form
														method="post"
														action={`/dash/${databaseId}/delete-shard`}
														style="display: inline;"
													>
														<input
															type="hidden"
															name="table_name"
															value={shard.table_name}
														/>
														<input
															type="hidden"
															name="shard_id"
															value={shard.shard_id}
														/>
														<button
															type="submit"
															class="text-black hover:text-red-600 transition cursor-pointer"
															title="Delete shard"
														>
															<i class="ph ph-x font-bold text-xl"></i>
														</button>
													</form>
												)}
											</td>
										</tr>
									);
								})}
							</tbody>
						</table>
					</div>
				)}
			</div>

			{/* Async Jobs Section */}
			<div class="bg-white border-2 border-black p-6 mb-8">
				<h2 class="text-sm font-semibold text-black uppercase tracking-wide mb-4">
					Async Jobs
				</h2>
				{!topology.async_jobs || topology.async_jobs.length === 0 ? (
					<p class="text-center text-black text-sm py-8">No async jobs</p>
				) : (
					<div class="overflow-x-auto">
						<table class="w-full text-sm border-collapse">
							<thead>
								<tr class="border-b-2 border-black">
									<th class="text-left px-4 py-3 font-semibold text-black">
										Job ID
									</th>
									<th class="text-left px-4 py-3 font-semibold text-black">
										Type
									</th>
									<th class="text-left px-4 py-3 font-semibold text-black">
										Table
									</th>
									<th class="text-left px-4 py-3 font-semibold text-black">
										Status
									</th>
									<th class="text-left px-4 py-3 font-semibold text-black">
										Started
									</th>
									<th class="text-left px-4 py-3 font-semibold text-black">
										Duration
									</th>
									<th class="text-left px-4 py-3 font-semibold text-black">
										Error
									</th>
								</tr>
							</thead>
							<tbody>
								{topology.async_jobs.map((job: AsyncJob) => {
									const startDate = new Date(job.started_at);
									const duration = job.duration_ms
										? `${(job.duration_ms / 1000).toFixed(2)}s`
										: "-";
									const statusColor =
										job.status === "completed"
											? "green"
											: job.status === "failed"
												? "red"
												: job.status === "running"
													? "blue"
													: "gray";

									return (
										<tr key={job.job_id} class="border-b border-black">
											<td
												class="px-4 py-3 font-mono text-xs text-black"
												title={job.job_id}
											>
												{job.job_id.substring(0, 8)}...
											</td>
											<td class="px-4 py-3 text-black">{job.job_type}</td>
											<td class="px-4 py-3 text-black">{job.table_name}</td>
											<td class="px-4 py-3">
												<span
													class={`inline-block px-2 py-1 text-xs font-medium badge badge-${statusColor}`}
												>
													{job.status}
												</span>
											</td>
											<td class="px-4 py-3 text-xs text-black">
												{startDate.toLocaleString()}
											</td>
											<td class="px-4 py-3 text-black text-xs">{duration}</td>
											<td
												class="px-4 py-3 text-xs text-black max-w-xs truncate"
												title={job.error_message || ""}
											>
												{job.error_message || "-"}
											</td>
										</tr>
									);
								})}
							</tbody>
						</table>
					</div>
				)}
			</div>
		</div>
	</>
);
