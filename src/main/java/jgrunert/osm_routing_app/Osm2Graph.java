package jgrunert.osm_routing_app;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.openstreetmap.osmosis.core.container.v0_6.EntityContainer;
import org.openstreetmap.osmosis.core.domain.v0_6.Entity;
import org.openstreetmap.osmosis.core.domain.v0_6.Node;
import org.openstreetmap.osmosis.core.domain.v0_6.Tag;
import org.openstreetmap.osmosis.core.domain.v0_6.Way;
import org.openstreetmap.osmosis.core.domain.v0_6.WayNode;
import org.openstreetmap.osmosis.core.task.v0_6.RunnableSource;
import org.openstreetmap.osmosis.core.task.v0_6.Sink;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;

/**
 * Processes OSM input to graph, parses input file, pre filtering, searches edges, calculate distances, clean up graph.
 *
 * @author Jonas Grunert
 *
 */
public class Osm2Graph {

	private int processedOsmElementsTmp = 0;
	private int totalOsmElements = 0;
	private int totalWayCount = 0;
	private int totalNodeCount = 0;

	// Temporary storage of ways, later converted to edges
	// Mapping from long OSM ID to integer assigned ID
	private Long2IntMap relevantNodeIdMap = new Long2IntOpenHashMap();
	private List<HighwayInfo> relevantWays = new ArrayList<>();

	// Nodes, converted from OSM nodes
	private int wayNodeCount;
	private int wayNodesFound = 0;
	private double[] wayNodeLat;
	private double[] wayNodeLon;
	private IntArrayList[] wayNodeOutgoingEdgeIDs;
	private IntArrayList[] wayNodeIncomingEdgeIDs;
	//private IntArrayList[] relevantNodeEdgeTargets;

	// Edges, convered from OSM ways
	private int edgeCount;
	private int[] wayEdgeNodes0;
	private int[] wayEdgeNodes1;
	private byte[] wayEdgeInfoBits;
	private short[] wayEdgeMaxSpeeds;
	private double[] wayEdgeLengths;

	private double minSpeed = 5; // TODO Configurable
	private double maxSpeed = 130; // TODO Configurable


	public void doProcessing(String inFile, String outDir, boolean reduceL2Nodes) throws Exception {
		OsmAppPreprocessor.LOG.info("OSM Preprocessor Pass1");
		long startTime = System.currentTimeMillis();

		// List of all highways, assign Int32 Ids to edges and nodes
		// Step 1 - read highways
		readWays(inFile, outDir);
		System.out.println("# Finished step 1/x - readWays");

		// Read coordinates of all points which are waypoints
		readWaypointCoords(inFile, outDir);
		System.out.println("# Finished step 2/x - readWaypointCoords");

		// Calculate all edges from ways
		calculateWayEdges();
		// Can throw away temporary highways and node mappings now, already converted ways to edges
		relevantNodeIdMap = null;
		relevantWays = null;
		System.out.println("# Finished step 3/x - calculateWayEdges");

		// Find all edges of each node
		findEdgeNodes();
		System.out.println("# Finished step 4/x - findEdgeNodes");

		// Remove duplicate edges and circle edges
		removeRedundantEdges();
		System.out.println("# Finished step 5/x - findEdgeNodes");

		// Remove Level-2 nodes
		if (reduceL2Nodes) {
			removeLevel2Nodes();
			System.out.println("# Finished step 6/x - removeLevel2Nodes");
		}
		else {
			System.out.println("# Skipped step 6/x - removeLevel2Nodes");
		}

		// Write
		outputBinary(outDir + File.separator + "graph.bin");
		System.out.println("# Finished step 7/x - output");

		OsmAppPreprocessor.LOG.info("Processing finished in " + (System.currentTimeMillis() - startTime) + "ms");
	}



	/**
	 * Reads and sorts all ways
	 */
	private void readWays(String inFile, String outDir) throws FileNotFoundException, IOException {
		OsmAppPreprocessor.LOG.info("Starting readWays");

		long startTime = System.currentTimeMillis();
		Sink sinkImplementation = new Sink() {

			@Override
			public void process(EntityContainer entityContainer) {
				Entity entity = entityContainer.getEntity();

				if (entity instanceof Way) {
					totalWayCount++;
					Way way = (Way) entity;

					String highway = null;
					String maxspeed = null;
					String sidewalk = null;
					String oneway = null;
					String access = null;
					for (Tag tag : way.getTags()) {
						if (tag.getKey().equals("highway")) {
							highway = tag.getValue();
						}
						else if (tag.getKey().equals("maxspeed")) {
							maxspeed = tag.getValue();
						}
						else if (tag.getKey().equals("sidewalk")) {
							sidewalk = tag.getValue();
						}
						else if (tag.getKey().equals("oneway")) {
							oneway = tag.getValue();
						}
						else if (tag.getKey().equals("access")) {
							access = tag.getValue();
						}
					}

					boolean accessOk;
					if (access != null && (access.equals("private") || access.equals("no") || access.equals("emergency")
							|| access.equals("agricultural") || access.equals("bus"))) {
						accessOk = false;
					}
					else {
						accessOk = true;
					}

					try {
						if (highway != null && accessOk) {
							HighwayInfo hw = evaluateHighway(highway, maxspeed, sidewalk, oneway);
							if (hw != null) {
								// Register all nodes of this way
								List<WayNode> wayNodes = way.getWayNodes();
								hw.wayNodeIds = new long[wayNodes.size()];
								for (int i = 0; i < wayNodes.size(); i++) {
									WayNode waynode = wayNodes.get(i);

									// Assign node ID if new node
									if (!relevantNodeIdMap.containsKey(waynode.getNodeId()))
										relevantNodeIdMap.put(waynode.getNodeId(), relevantNodeIdMap.size());

									// Remember way node
									hw.wayNodeIds[i] = waynode.getNodeId();
								}

								relevantWays.add(hw);
							}
						}
					}
					catch (Exception e) {
						e.printStackTrace();
					}
				}
				else if (entity instanceof Node) {
					totalNodeCount++;
				}

				processedOsmElementsTmp++;
				if ((processedOsmElementsTmp % 1000000) == 0) {
					System.out.println(String.format("Pass 1a Loaded %d elements, %d nodes, %d ways", processedOsmElementsTmp,
							totalNodeCount, totalWayCount));
				}
			}

			@Override
			public void release() {
			}

			@Override
			public void complete() {
			}

			@Override
			public void initialize(Map<String, Object> arg0) {

			}
		};

		RunnableSource reader;
		try {
			reader = new crosby.binary.osmosis.OsmosisReader(new BufferedInputStream(new FileInputStream(new File(inFile))));
		}
		catch (FileNotFoundException e1) {
			e1.printStackTrace();
			//			highwaysTempWriter.close();
			return;
		}
		reader.setSink(sinkImplementation);

		Thread readerThread = new Thread(reader);
		readerThread.start();

		while (readerThread.isAlive()) {
			try {
				readerThread.join();
			}
			catch (InterruptedException e) {
				e.printStackTrace();
				//				highwaysTempWriter.close();
				return;
			}
		}

		//		highwaysTempWriter.close();

		// highwayCsvWriter.close();

		OsmAppPreprocessor.LOG.info(String.format("Finished readWays - loading Nodes %d/%d Ways %d/%d", totalNodeCount,
				relevantNodeIdMap.size(), totalWayCount, relevantWays.size()));
		OsmAppPreprocessor.LOG.info("Time elapsed: " + (System.currentTimeMillis() - startTime) + "ms");
		totalOsmElements = processedOsmElementsTmp;
	}



	/**
	 * Reads all and sorts all waypoints coordinates - nodes which are a node involved in a relevant way
	 */
	private void readWaypointCoords(String inFile, String outDir) throws FileNotFoundException, IOException {
		wayNodeLat = new double[relevantNodeIdMap.size()];
		wayNodeLon = new double[relevantNodeIdMap.size()];
		//		wayNodes0 = new int[relevantWayIdMap.size()];
		//		wayNodes1 = new int[relevantWayIdMap.size()];

		OsmAppPreprocessor.LOG.info("Starting readWaypointCoords");
		processedOsmElementsTmp = 0;
		long startTime = System.currentTimeMillis();
		Sink sinkImplementation = new Sink() {

			@Override
			public void process(EntityContainer entityContainer) {
				Entity entity = entityContainer.getEntity();

				if (entity instanceof Node) {
					Node node = (Node) entity;
					if (relevantNodeIdMap.containsKey(node.getId())) {
						int id = relevantNodeIdMap.get(node.getId());
						wayNodeLat[id] = node.getLatitude();
						wayNodeLon[id] = node.getLongitude();
						wayNodesFound++;
					}
				}

				processedOsmElementsTmp++;
				if ((processedOsmElementsTmp % 1000000) == 0) {
					System.out.println(String.format("Pass 1b Loaded %d elements, %.2f", processedOsmElementsTmp,
							(double) processedOsmElementsTmp / totalOsmElements * 100) + "%");
				}
			}

			@Override
			public void release() {
			}

			@Override
			public void complete() {
			}

			@Override
			public void initialize(Map<String, Object> arg0) {

			}
		};

		RunnableSource reader;
		try {
			reader = new crosby.binary.osmosis.OsmosisReader(new BufferedInputStream(new FileInputStream(new File(inFile))));
		}
		catch (FileNotFoundException e1) {
			e1.printStackTrace();
			//			highwaysTempWriter.close();
			return;
		}
		reader.setSink(sinkImplementation);

		Thread readerThread = new Thread(reader);
		readerThread.start();

		while (readerThread.isAlive()) {
			try {
				readerThread.join();
			}
			catch (InterruptedException e) {
				e.printStackTrace();
				//				highwaysTempWriter.close();
				return;
			}
		}

		//		highwaysTempWriter.close();

		// highwayCsvWriter.close();

		if (wayNodesFound == relevantNodeIdMap.size()) OsmAppPreprocessor.LOG.info("All relevant nodes found");
		else OsmAppPreprocessor.LOG.info("Not all relevant nodes found, only " + wayNodesFound + " of " + relevantNodeIdMap.size());
		wayNodeCount = relevantNodeIdMap.size();

		OsmAppPreprocessor.LOG.info("Finished readWaypointCoords. Time elapsed: " + (System.currentTimeMillis() - startTime) + "ms");
	}


	/**
	 * Finds and calculates all edges on ways
	 */
	private void calculateWayEdges() {
		OsmAppPreprocessor.LOG.info("Starting calculateWayEdges");
		long startTime = System.currentTimeMillis();

		edgeCount = 0;
		for (HighwayInfo hw : relevantWays) {
			if (hw.Oneway) edgeCount += hw.wayNodeIds.length - 1;
			else edgeCount += (hw.wayNodeIds.length - 1) * 2;
		}
		OsmAppPreprocessor.LOG.info("edgeCount: " + edgeCount);

		wayEdgeNodes0 = new int[edgeCount];
		wayEdgeNodes1 = new int[edgeCount];
		wayEdgeInfoBits = new byte[edgeCount];
		wayEdgeMaxSpeeds = new short[edgeCount];
		wayEdgeLengths = new double[edgeCount];

		int iEdge = 0;
		for (HighwayInfo hw : relevantWays) {
			for (int iWp = 1; iWp < hw.wayNodeIds.length; iWp++) {
				int wp0 = relevantNodeIdMap.get(hw.wayNodeIds[iWp - 1]);
				int wp1 = relevantNodeIdMap.get(hw.wayNodeIds[iWp]);
				double dist = calcGeoLength(wp0, wp1);

				evaluateEdge(wp0, wp1, dist, hw, iEdge);
				iEdge++;
				if (!hw.Oneway) {
					evaluateEdge(wp1, wp0, dist, hw, iEdge);
					iEdge++;
				}
			}
		}

		OsmAppPreprocessor.LOG.info("Finished calculateWayEdges. Time elapsed: " + (System.currentTimeMillis() - startTime) + "ms");
	}

	private void evaluateEdge(int wp0, int wp1, double dist, HighwayInfo hw, int iEdge) {
		if (wp0 == 0) {
			System.out.println();
		}

		wayEdgeNodes0[iEdge] = wp0;
		wayEdgeNodes1[iEdge] = wp1;
		wayEdgeInfoBits[iEdge] = hw.InfoBits;
		wayEdgeMaxSpeeds[iEdge] = hw.MaxSpeed;
		wayEdgeLengths[iEdge] = dist;
	}


	/**
	 * Finds all edges of each node. Creates relevantNodeEdgeIDs which is quite RAM consuming.
	 */
	private void findEdgeNodes() {
		OsmAppPreprocessor.LOG.info("findEdgeNodes starting");

		wayNodeOutgoingEdgeIDs = new IntArrayList[wayNodeCount];
		wayNodeIncomingEdgeIDs = new IntArrayList[wayNodeCount];
		//		relevantNodeEdgeTargets = new IntArrayList[relevantNodeCount];
		for (int iNode = 0; iNode < wayNodeCount; iNode++) {
			wayNodeOutgoingEdgeIDs[iNode] = new IntArrayList(1);
			wayNodeIncomingEdgeIDs[iNode] = new IntArrayList(1);
			//			relevantNodeEdgeTargets[iNode] = new IntArrayList();

			if ((iNode % 1000000) == 0) {
				System.out.println(String.format("relevantNodeCount create lists %.2f", (double) iNode / wayNodeCount * 100) + "%");
			}
		}

		for (int iEdge = 0; iEdge < edgeCount; iEdge++) {
			int edgeSource = wayEdgeNodes0[iEdge];
			int edgeTarget = wayEdgeNodes1[iEdge];

			wayNodeOutgoingEdgeIDs[edgeSource].add(iEdge);
			wayNodeIncomingEdgeIDs[edgeTarget].add(iEdge);

			if ((iEdge % 1000000) == 0) {
				System.out.println(String.format("relevantNodeCount find edge nodes %.2f", (double) iEdge / edgeCount * 100) + "%");
			}
		}

		OsmAppPreprocessor.LOG.info("findEdgeNodes finished, " + edgeCount + " edges");
	}


	/**
	 * Removes duplicate and circle edges
	 */
	private void removeRedundantEdges() {
		OsmAppPreprocessor.LOG.info("removeRedundantEdges starting");

		int duplicateEdges = 0;
		int circleEdges = 0;

		HashSet<Integer> targetsTmp = new HashSet<>();
		for (int iNode = 0; iNode < wayNodeCount; iNode++) {
			IntArrayList outEdges = wayNodeOutgoingEdgeIDs[iNode];
			for (int iEdge = 0; iEdge < outEdges.size(); iEdge++) {
				int edge = outEdges.getInt(iEdge);

				//				int target;
				//				if (wayEdgeNodes0[edge] == iNode) target = wayEdgeNodes1[edge];
				//				else target = wayEdgeNodes0[edge];
				assert (wayEdgeNodes0[edge] == iNode);
				int target = wayEdgeNodes1[edge];

				if (target == iNode) {
					circleEdges++;
					outEdges.removeInt(iEdge);
					IntArrayList inEdges = wayNodeIncomingEdgeIDs[iNode];
					inEdges.rem(edge);
					iEdge--;
				}
				else if (targetsTmp.contains(target)) {
					duplicateEdges++;
					outEdges.removeInt(iEdge);
					IntArrayList targetInEdges = wayNodeIncomingEdgeIDs[target];
					targetInEdges.rem(edge);
					iEdge--;
				}
				else {
					targetsTmp.add(target);
				}
			}
			targetsTmp.clear();

			if ((iNode % 1000000) == 0) {
				System.out.println(String.format("removeRedundantEdges %.2f", (double) iNode / wayNodeCount * 100) + "%");
			}
		}

		OsmAppPreprocessor.LOG
				.info("removeRedundantEdges finished, removed " + duplicateEdges + " duplicates and " + circleEdges + " circles");
	}


	/**
	 * Removes all nodes which are no crosses and no dead ends - all nodes which only bridge between two other nodes.
	 * Does not remove if edges have different maxSpeeds. TODO Test number
	 */
	private void removeLevel2Nodes() {
		OsmAppPreprocessor.LOG.info("removeLevel2Nodes starting");

		int removedUnidirL2s = 0;
		int removedBidirL2s = 0;

		for (int iNode = 0; iNode < wayNodeCount; iNode++) {
			IntArrayList outEdges = wayNodeOutgoingEdgeIDs[iNode];
			IntArrayList inEdges = wayNodeIncomingEdgeIDs[iNode];

			if (outEdges.size() == 1 && inEdges.size() == 1) {
				// No crossing, unidirectional nodes
				int inEdge = inEdges.getInt(0);
				int outEdge = outEdges.getInt(0);
				int inEdgeSrcNode = wayEdgeNodes0[inEdge];
				int outEdgeTargetNode = wayEdgeNodes1[outEdge];

				assert (wayEdgeNodes1[inEdge] == iNode);
				assert (wayEdgeNodes0[outEdge] == iNode);
				assert (wayEdgeNodes0[inEdge] != iNode);
				assert (wayEdgeNodes1[outEdge] != iNode);

				// Merge edge if level2-node, if node is no dead end (in and out edge with not same node) and edges have similar speeds
				if (inEdgeSrcNode != outEdgeTargetNode && wayEdgeMaxSpeeds[inEdge] == wayEdgeMaxSpeeds[outEdge]) { // Ignoring wayEdgeInfoBits
					// Merge edges
					wayEdgeLengths[inEdge] += wayEdgeLengths[outEdge];
					wayEdgeNodes1[inEdge] = outEdgeTargetNode;
					// Update incoming edge at target node
					int targetNodeEdgeIndex = findIndexOf(wayNodeIncomingEdgeIDs[outEdgeTargetNode], outEdge);
					assert (targetNodeEdgeIndex != -1);
					wayNodeIncomingEdgeIDs[outEdgeTargetNode].set(targetNodeEdgeIndex, inEdge);

					wayNodeOutgoingEdgeIDs[iNode] = null;
					wayNodeIncomingEdgeIDs[iNode] = null;
					removedUnidirL2s++;
				}
				//				else {
				//					diffSpeedF++;
				//				}
			}
			else if (outEdges.size() == 2 && inEdges.size() == 2) {
				int inEdge0 = inEdges.getInt(0);
				int inEdge1 = inEdges.getInt(1);
				int inEdgeSrcNode0 = wayEdgeNodes0[inEdge0];
				int inEdgeSrcNode1 = wayEdgeNodes0[inEdge1];

				int outEdge0;
				int outEdge1;
				if (wayEdgeNodes1[outEdges.getInt(1)] == inEdgeSrcNode1) {
					outEdge0 = outEdges.getInt(1);
					outEdge1 = outEdges.getInt(0);
				}
				else {
					outEdge0 = outEdges.getInt(0);
					outEdge1 = outEdges.getInt(1);
				}
				int outEdgeTargetNode0 = wayEdgeNodes1[outEdge0];
				int outEdgeTargetNode1 = wayEdgeNodes1[outEdge1];

				// Correct edges
				assert (wayEdgeNodes1[inEdge0] == iNode);
				assert (wayEdgeNodes1[inEdge1] == iNode);
				assert (wayEdgeNodes0[outEdge0] == iNode);
				assert (wayEdgeNodes0[outEdge1] == iNode);

				//				System.out.println((inEdgeSrcNode0 == outEdgeTargetNode1 && inEdgeSrcNode1 == outEdgeTargetNode0) + " "
				//						+ (inEdgeSrcNode0 == outEdgeTargetNode1 && inEdgeSrcNode1 == outEdgeTargetNode0
				//								&& wayEdgeMaxSpeeds[inEdge0] == wayEdgeMaxSpeeds[inEdge1]
				//								&& wayEdgeMaxSpeeds[inEdge0] == wayEdgeMaxSpeeds[outEdge0]
				//								&& wayEdgeMaxSpeeds[inEdge0] == wayEdgeMaxSpeeds[outEdge1])
				//						+ ": " + inEdgeSrcNode0 + " " + outEdgeTargetNode1 + " " + inEdgeSrcNode1 + " " + outEdgeTargetNode0);

				// Merge edge if
				//  - level2-node, has only two bidirectional connections with other nodes
				//  - edges dont form a loop
				//  - edges have similar speeds
				if (inEdgeSrcNode0 == outEdgeTargetNode1 && inEdgeSrcNode1 == outEdgeTargetNode0 && inEdgeSrcNode0 != iNode
						&& inEdgeSrcNode1 != iNode && wayEdgeMaxSpeeds[inEdge0] == wayEdgeMaxSpeeds[inEdge1]
						&& wayEdgeMaxSpeeds[inEdge0] == wayEdgeMaxSpeeds[outEdge0]
						&& wayEdgeMaxSpeeds[inEdge0] == wayEdgeMaxSpeeds[outEdge1]) { // Ignoring wayEdgeInfoBits
					// Merge edges0
					wayEdgeLengths[inEdge0] += wayEdgeLengths[outEdge0];
					wayEdgeNodes1[inEdge0] = outEdgeTargetNode0;
					int targetNodeEdgeIndex0 = findIndexOf(wayNodeIncomingEdgeIDs[outEdgeTargetNode0], outEdge0);
					assert (targetNodeEdgeIndex0 != -1);
					wayNodeIncomingEdgeIDs[outEdgeTargetNode0].set(targetNodeEdgeIndex0, inEdge0);

					if (wayNodeIncomingEdgeIDs[outEdgeTargetNode1] == null) System.out.println(outEdgeTargetNode1 + " " + iNode);

					// Merge edges1
					wayEdgeLengths[inEdge1] += wayEdgeLengths[outEdge1];
					wayEdgeNodes1[inEdge1] = outEdgeTargetNode1;
					int targetNodeEdgeIndex1 = findIndexOf(wayNodeIncomingEdgeIDs[outEdgeTargetNode1], outEdge1);
					assert (targetNodeEdgeIndex1 != -1);
					wayNodeIncomingEdgeIDs[outEdgeTargetNode1].set(targetNodeEdgeIndex1, inEdge1);

					//					System.out.println(wayEdgeLengths[inEdge0] + " " + wayEdgeLengths[inEdge1]);

					wayNodeOutgoingEdgeIDs[iNode] = null;
					wayNodeIncomingEdgeIDs[iNode] = null;
					removedBidirL2s++;
				}
			}

			if ((iNode % 1000000) == 0) {
				System.out.println(String.format("removeLevel2Nodes %.2f", (double) iNode / wayNodeCount * 100) + "%");
			}
		}

		OsmAppPreprocessor.LOG.info("removeLevel2Nodes finished, removed " + removedUnidirL2s + " removedUnidirL2s and " + removedBidirL2s
				+ " removedBidirL2s");

	}


	private void outputText(String outFile) {
		OsmAppPreprocessor.LOG.info("output starting");

		StringBuilder sb = new StringBuilder();
		try (PrintWriter writer = new PrintWriter(outFile)) {
			for (int iNode = 0; iNode < wayNodeCount; iNode++) {
				IntArrayList outEdges = wayNodeOutgoingEdgeIDs[iNode];
				if (isRelevantWayNode(iNode)) {
					sb.append(iNode);
					sb.append(';');
					sb.append(wayNodeLat[iNode]);
					sb.append(';');
					sb.append(wayNodeLon[iNode]);
					sb.append('|');

					if (outEdges != null) {
						for (int edge : outEdges) {
							int otherNode = getOtherEdgeNode(edge, iNode);
							sb.append(otherNode);
							sb.append(';');
							// Edge length: Time in seconds
							sb.append(getEdgeTime(edge));
							sb.append('|');
						}
					}

					writer.println(sb.toString());
					sb.setLength(0);
				}

				if ((iNode % 1000000) == 0) {
					System.out.println(String.format("output %.2f", (double) iNode / wayNodeCount * 100) + "%");
				}
			}
		}
		catch (FileNotFoundException e) {
			OsmAppPreprocessor.LOG.severe("Failure at main: " + e);
			e.printStackTrace();
		}

		OsmAppPreprocessor.LOG.info("output finished");
	}


	private void outputBinary(String outFile) {
		OsmAppPreprocessor.LOG.info("output starting");

		int nodesWritten = 0;
		int edgesWritten = 0;

		try (DataOutputStream writer = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(outFile)))) {
			int relevantWayNodeCount = 0;
			for (int iNode = 0; iNode < wayNodeCount; iNode++) {
				if (isRelevantWayNode(iNode)) {
					relevantWayNodeCount++;
				}
			}

			writer.writeInt(relevantWayNodeCount);

			for (int iNode = 0; iNode < wayNodeCount; iNode++) {
				if (isRelevantWayNode(iNode)) {
					IntArrayList outEdges = wayNodeOutgoingEdgeIDs[iNode];
					writer.writeInt(iNode);
					writer.writeDouble(wayNodeLat[iNode]);
					writer.writeDouble(wayNodeLon[iNode]);

					if (outEdges != null) {
						writer.writeInt(outEdges.size());
						for (int edge : outEdges) {
							int otherNode = getOtherEdgeNode(edge, iNode);
							writer.writeInt(otherNode);
							// Edge length: Time in seconds
							writer.writeDouble(getEdgeTime(edge));
						}
						edgesWritten += outEdges.size();
					}
					else {
						writer.writeInt(0);
					}
					nodesWritten++;
				}

				if ((iNode % 1000000) == 0) {
					System.out.println(String.format("output %.2f", (double) iNode / wayNodeCount * 100) + "%");
				}
			}
		}
		catch (Exception e) {
			OsmAppPreprocessor.LOG.severe("Failure at main: " + e);
			e.printStackTrace();
		}

		OsmAppPreprocessor.LOG.info("output finished, " + nodesWritten + " nodesWritten " + edgesWritten + " edgesWritten");
	}

	private boolean isRelevantWayNode(int iNode) {
		IntArrayList outEdges = wayNodeOutgoingEdgeIDs[iNode];
		IntArrayList inEdges = wayNodeIncomingEdgeIDs[iNode];
		return (outEdges != null && outEdges.size() > 0) || (inEdges != null && inEdges.size() > 0);
	}

	private double getEdgeTime(int edge) {
		return wayEdgeLengths[edge] / (Math.max(Math.min(wayEdgeMaxSpeeds[edge], maxSpeed), minSpeed) / 3.6);
	}

	private int getOtherEdgeNode(int edge, int thisNodeId) {
		if (wayEdgeNodes0[edge] == thisNodeId) return wayEdgeNodes1[edge];
		else return wayEdgeNodes0[edge];
	}



	private static class HighwayInfo implements Serializable {

		private static final long serialVersionUID = -1195125473333000206L;

		/** Info bits, bit0: Pedestrian, bit1: Car **/
		public final byte InfoBits;
		public final boolean Oneway;
		public final short MaxSpeed;
		public long[] wayNodeIds;


		public HighwayInfo(boolean car, boolean pedestrian, boolean oneway, short maxSpeed) {
			byte infoBitsTmp = car ? (byte) 1 : (byte) 0;
			infoBitsTmp = (byte) (infoBitsTmp << 1);
			infoBitsTmp += pedestrian ? (byte) 1 : (byte) 0;
			this.InfoBits = infoBitsTmp;
			this.Oneway = oneway;
			this.MaxSpeed = maxSpeed;
		}
	}


	static final int SPEED_WALK = 0;
	static final int SPEED_LIVINGSTREET = 5;
	static final int SPEED_UNLIMITED = 255;

	enum SidwalkMode {
		Yes, No, Unspecified
	};

	private static HighwayInfo evaluateHighway(String highwayTag, String maxspeedTag, String sidewalkTag, String onewayTag) {

		String originalMaxSpeed = maxspeedTag;

		// Try to find out maxspeed
		Short maxSpeed;
		if (maxspeedTag != null) {
			if (maxspeedTag.equals("none") || maxspeedTag.equals("signals") || maxspeedTag.equals("variable")
					|| maxspeedTag.equals("unlimited")) {
				// No speed limitation
				maxSpeed = SPEED_UNLIMITED;
			}
			else if (maxspeedTag.contains("living_street")) {
				maxSpeed = SPEED_LIVINGSTREET;
			}
			else if (maxspeedTag.contains("walk") || maxspeedTag.contains("foot")) {
				maxSpeed = SPEED_WALK;
			}
			else {
				try {
					boolean mph = false;
					// Try to parse speed limit
					if (maxspeedTag.contains("mph")) {
						mph = true;
						maxspeedTag = maxspeedTag.replace("mph", "");
					}
					if (maxspeedTag.contains("km/h")) maxspeedTag = maxspeedTag.replace("km/h", "");
					if (maxspeedTag.contains("kmh")) maxspeedTag = maxspeedTag.replace("kmh", "");
					if (maxspeedTag.contains(".")) maxspeedTag = maxspeedTag.split("\\.")[0];
					if (maxspeedTag.contains(",")) maxspeedTag = maxspeedTag.split(",")[0];
					if (maxspeedTag.contains(";")) maxspeedTag = maxspeedTag.split(";")[0];
					if (maxspeedTag.contains("-")) maxspeedTag = maxspeedTag.split("-")[0];
					if (maxspeedTag.contains(" ")) maxspeedTag = maxspeedTag.split(" ")[0];

					maxSpeed = Short.parseShort(maxspeedTag);
					if (mph) {
						maxSpeed = (short) (maxSpeed * 1.60934);
					}
				}
				catch (Exception e) {
					OsmAppPreprocessor.LOG.finest("Illegal maxspeed: " + originalMaxSpeed);
					maxSpeed = null;
				}
			}
		}
		else {
			maxSpeed = null;
		}


		// Try to find out if has sidewalk
		SidwalkMode sidewalk = SidwalkMode.Unspecified;
		if (sidewalkTag != null) {
			if (sidewalkTag.equals("no") || sidewalkTag.equals("none")) {
				sidewalk = SidwalkMode.No;
			}
			else {
				sidewalk = SidwalkMode.Yes;
			}
		}


		// Try to find out if is oneway
		Boolean oneway;
		if (onewayTag != null) {
			oneway = onewayTag.equals("yes");
		}
		else {
			oneway = null;
		}


		// Try to classify highway
		if (highwayTag.equals("track")) {
			// track
			if (maxSpeed == null) maxSpeed = 10;
			if (oneway == null) oneway = false;

			return new HighwayInfo(true, true, oneway, maxSpeed);
		}
		else if (highwayTag.equals("residential")) {
			// residential road
			if (maxSpeed == null) maxSpeed = 50;
			if (oneway == null) oneway = false;

			// Residential by default with sideway
			return new HighwayInfo(true, (sidewalk != SidwalkMode.No), oneway, maxSpeed);
		}
		else if (highwayTag.equals("service")) {
			// service road
			if (maxSpeed == null) maxSpeed = 30;
			if (oneway == null) oneway = false;

			// Residential by default with sideway
			return new HighwayInfo(true, (sidewalk != SidwalkMode.No), oneway, maxSpeed);
		}
		else if (highwayTag.equals("footway") || highwayTag.equals("path") || highwayTag.equals("steps") || highwayTag.equals("bridleway")
				|| highwayTag.equals("pedestrian")) {
			// footway etc.
			if (maxSpeed == null) maxSpeed = 0;
			if (oneway == null) oneway = false;

			return new HighwayInfo(false, true, oneway, maxSpeed);
		}
		else if (highwayTag.startsWith("primary")) {
			// country road etc
			if (maxSpeed == null) maxSpeed = 100;
			if (oneway == null) oneway = false;

			return new HighwayInfo(true, (sidewalk == SidwalkMode.Yes), oneway, maxSpeed);
		}
		else if (highwayTag.startsWith("secondary") || highwayTag.startsWith("tertiary")) {
			// country road etc
			if (maxSpeed == null) maxSpeed = 100;
			if (oneway == null) oneway = false;

			return new HighwayInfo(true, (sidewalk != SidwalkMode.No), oneway, maxSpeed);
		}
		else if (highwayTag.equals("unclassified")) {
			// unclassified (small road)
			if (maxSpeed == null) maxSpeed = 50;
			if (oneway == null) oneway = false;

			return new HighwayInfo(true, (sidewalk != SidwalkMode.No), oneway, maxSpeed);
		}
		else if (highwayTag.equals("living_street")) {
			// living street
			if (maxSpeed == null) maxSpeed = SPEED_LIVINGSTREET;
			if (oneway == null) oneway = false;

			return new HighwayInfo(true, true, oneway, maxSpeed);
		}
		else if (highwayTag.startsWith("motorway")) {
			// track
			if (maxSpeed == null) maxSpeed = 255;
			if (oneway == null) oneway = true;

			return new HighwayInfo(true, (sidewalk == SidwalkMode.Yes), oneway, maxSpeed);
		}
		else if (highwayTag.startsWith("trunk")) {
			// trunk road
			if (maxSpeed == null) maxSpeed = 255;
			if (oneway == null) oneway = false;

			return new HighwayInfo(true, (sidewalk == SidwalkMode.Yes), oneway, maxSpeed);
		}

		// Ignore this road if no useful classification available
		return null;
	}



	private double calcGeoLength(int i0, int i1) {
		double dist = getNodeDist(wayNodeLat[i0], wayNodeLon[i0], wayNodeLat[i1], wayNodeLon[i1]);
		return dist;
	}

	private static double getNodeDist(double lat1, double lon1, double lat2, double lon2) {
		double earthRadius = 6371000; // meters
		double dLat = Math.toRadians(lat2 - lat1);
		double dLng = Math.toRadians(lon2 - lon1);
		double a = Math.sin(dLat / 2) * Math.sin(dLat / 2)
				+ Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) * Math.sin(dLng / 2) * Math.sin(dLng / 2);
		double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
		return (earthRadius * c);
	}

	private static int findIndexOf(IntArrayList arr, int value) {
		for (int i = 0; i < arr.size(); i++) {
			if (arr.getInt(i) == value) return i;
		}
		return -1;
	}
}
