/**
 * Home page component - create or load a topology
 */
export const HomePage = () => (
	<>
		<title>Big Daddy Dashboard</title>

		<div class="py-12 border-b-2 border-black">
			<h1 class="text-3xl font-bold tracking-tight text-black">Big Daddy</h1>
			<p class="mt-2 text-sm text-black">
				Distributed SQL Database on Cloudflare Workers
			</p>
		</div>

		<div class="max-w-md mx-auto my-12">
			<div class="bg-white border-2 border-black p-8">
				<h2 class="text-lg font-semibold text-black mb-6">Access Database</h2>
				<form method="get" action="/dash" class="space-y-5">
					<div>
						<label
							for="database-id"
							class="block text-sm font-medium text-black mb-2"
						>
							Database Name
						</label>
						<input
							type="text"
							id="database-id"
							name="id"
							placeholder="Enter database name"
							required
							class="w-full px-3 py-2 border-2 border-black text-sm focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-black"
						/>
						<p class="mt-1 text-xs text-black">
							Enter an existing database ID or a new one to create it.
						</p>
					</div>

					<div>
						<label
							for="node-count"
							class="block text-sm font-medium text-black mb-2"
						>
							Storage Nodes
						</label>
						<input
							type="number"
							id="node-count"
							name="nodes"
							min="1"
							max="100"
							value="3"
							class="w-full px-3 py-2 border-2 border-black text-sm focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-black"
						/>
						<p class="mt-1 text-xs text-black">
							Number of storage nodes to create (used only for new databases,
							1-100).
						</p>
					</div>

					<button
						type="submit"
						class="w-full bg-black text-white font-medium py-2 hover:bg-white hover:text-black hover:border-2 hover:border-black border-2 border-black transition"
					>
						Access Dashboard
					</button>
				</form>
			</div>
		</div>
	</>
);
