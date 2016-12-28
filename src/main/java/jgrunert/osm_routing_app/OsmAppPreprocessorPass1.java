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
import java.util.logging.Level;

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
 * First processing pass, parses input file, pre filtering, searches edges.
 *
 * @author Jonas Grunert
 *
 */
public class OsmAppPreprocessorPass1 {

	//	int maxNodesPerWay = 0;
	//	int maxWaysPerNode = 0;

	// int elementsPass1 = 0;
	// int elementsPass2 = 0;

	//	int BUFFER_MAX_WAYNODES = 100000000;
	//	int BUFFER_MAX_WAYSPERNODE = 20;

	// static int relevantWayNodeCount = 0;

	// static LongSet relevantWays = new LongOpenHashSet();


	private int processedOsmElementsTmp = 0;
	private int totalOsmElements = 0;
	private int totalWayCount = 0;
	private int totalNodeCount = 0;

	// Map of all way nodes and their edges
	//private Long2ObjectMap<IntArrayList> relevantNodes = new Long2ObjectOpenHashMap<IntArrayList>();

	//private Long2IntMap relevantWayIdMap = new Long2IntOpenHashMap();

	// Temporary storage of ways, later converted to edges
	// Mapping from long OSM ID to integer assigned ID
	private Long2IntMap relevantNodeIdMap = new Long2IntOpenHashMap();
	private List<HighwayInfo> relevantWays = new ArrayList<>();

	// Nodes, converted from OSM nodes
	private int relevantNodeCount;
	private int relevantNodesFound = 0;
	private double[] relevantNodeLat;
	private double[] relevantNodeLon;
	private IntArrayList[] relevantNodeEdgeIDs;
	//private IntArrayList[] relevantNodeEdgeTargets;

	// Edges, convered from OSM ways
	private int edgeCount;
	private int[] wayEdgeNodes0;
	private int[] wayEdgeNodes1;
	private byte[] wayEdgeInfoBits;
	private short[] wayEdgeMaxSpeeds;
	private double[] wayEdgeLengths;


	// static List<HighwayInfos> highways = new ArrayList<>();
	// IDs of all relevant waypoint nodes
	// static List<Long> waypointIds = new ArrayList<>();


	public static void main(String[] args) {
		try {
			String outDir = args[0];
			String inFile = args[1];
			new OsmAppPreprocessorPass1().doPass(inFile, outDir);
		}
		catch (Exception e) {
			OsmAppPreprocessor.LOG.severe("Failure at main");
			OsmAppPreprocessor.LOG.log(Level.SEVERE, "Exception", e);
		}
	}


	public void doPass(String inFile, String outDir) throws Exception {
		OsmAppPreprocessor.LOG.info("OSM Preprocessor Pass1");
		long startTime = System.currentTimeMillis();

		// List of all highways, assign Int32 Ids to edges and nodes
		// Pass 1 - read highways
		readWays(inFile, outDir);

		readWaypointCoords(inFile, outDir);

		// Calculate all edges from ways
		calculateWayEdges();
		// Can throw away temporary highways and node mappings now, already converted ways to edges
		relevantNodeIdMap = null;
		relevantWays = null;

		// Find all edges of each node
		findEdgeNodes();

		// Remove duplicate edges and circle edges
		removeRedundantEdges();

		// TODO remove Level-2, remove unused

		outputBinary(outDir + File.separator + "graph.bin");


		OsmAppPreprocessor.LOG.info("Finished Pass 1. Time elapsed: " + (System.currentTimeMillis() - startTime) + "ms");



		OsmAppPreprocessor.LOG.info("Pass 1 finished");
		OsmAppPreprocessor.LOG.info("Relevant edges: " + edgeCount + ", total ways: " + totalWayCount);
		OsmAppPreprocessor.LOG.info("Relevant waynodes: " + relevantNodeCount + ", total nodes: " + totalNodeCount);
		//		OsmAppPreprocessor.LOG.info("Max nodes per way: " + maxNodesPerWay);
		//		OsmAppPreprocessor.LOG.info("Max ways per node: " + maxWaysPerNode);

		OsmAppPreprocessor.LOG.info("Finished in " + (System.currentTimeMillis() - startTime) + "ms");
	}



	/**
	 * Reads and sorts all ways
	 */
	private void readWays(String inFile, String outDir) throws FileNotFoundException, IOException {
		OsmAppPreprocessor.LOG.info("Starting Pass 1a");

		//		ObjectOutputStream highwaysTempWriter = new ObjectOutputStream(
		//				new BufferedOutputStream(new FileOutputStream(outDir + File.separator + "pass1-temp-highways.bin")));

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
								// Register OSM map ID and assign new id
								//								int edgeId = relevantWayIdMap.size();
								//								relevantWayIdMap.put(entity.getId(), edgeId);

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



								//for (WayNode waynode : wayNodes) {
								//									IntArrayList nodeWays = relevantNodes.get(waynode.getNodeId());
								//									if (nodeWays == null) {
								//										nodeWays = new IntArrayList();
								//										relevantNodes.put(waynode.getNodeId(), nodeWays);
								//										relevantNodeIdMap.put(waynode.getNodeId(), relevantNodeIdMap.size());
								//									}
								//									nodeWays.add(edgeId);



								// Short ways =
								// waysPerNode.get(waynode.getNodeId());
								// if(ways == null) { ways = 0; }
								// ways++;
								// OsmAppPreprocessor.LOG.info(ways);
								// maxWaysPerNode =
								// Math.max(maxWaysPerNode, ways);
								// waysPerNode.put(waynode.getNodeId(),
								// ways);
								// waynodes.add(waynode.getNodeId());

								// waypointIds.add(waynode.getNodeId());
								//}



								//								highwaysTempWriter.writeObject(hw);

								//maxNodesPerWay = Math.max(maxNodesPerWay, way.getWayNodes().size());
							}
							// else {
							// highwayCsvAllWriter.println(highway + ";"
							// + maxspeed + ";" + sidewalk + ";"
							// + oneway + ";Ignored;");
							// }
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

		OsmAppPreprocessor.LOG.info(String.format("Finished Pass 1a - loading Nodes %d/%d Ways %d/%d", totalNodeCount,
				relevantNodeIdMap.size(), totalWayCount, relevantWays.size()));
		OsmAppPreprocessor.LOG.info("Time elapsed: " + (System.currentTimeMillis() - startTime) + "ms");
		totalOsmElements = processedOsmElementsTmp;
	}



	/**
	 * Reads all and sorts all waypoints coordinates - nodes which are a node involved in a relevant way
	 */
	private void readWaypointCoords(String inFile, String outDir) throws FileNotFoundException, IOException {
		relevantNodeLat = new double[relevantNodeIdMap.size()];
		relevantNodeLon = new double[relevantNodeIdMap.size()];
		//		wayNodes0 = new int[relevantWayIdMap.size()];
		//		wayNodes1 = new int[relevantWayIdMap.size()];

		OsmAppPreprocessor.LOG.info("Starting Pass 1b readWaypointCoords");
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
						relevantNodeLat[id] = node.getLatitude();
						relevantNodeLon[id] = node.getLongitude();
						relevantNodesFound++;
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

		if (relevantNodesFound == relevantNodeIdMap.size()) OsmAppPreprocessor.LOG.info("All relevant nodes found");
		else OsmAppPreprocessor.LOG.info("Not all relevant nodes found, only " + relevantNodesFound + " of " + relevantNodeIdMap.size());
		relevantNodeCount = relevantNodeIdMap.size();

		OsmAppPreprocessor.LOG.info("Finished Pass 1b. Time elapsed: " + (System.currentTimeMillis() - startTime) + "ms");
	}


	/**
	 * Finds and calculates all edges on ways
	 */
	private void calculateWayEdges() {
		OsmAppPreprocessor.LOG.info("Starting Pass 1c calculateWayEdges");
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

		OsmAppPreprocessor.LOG.info("Finished Pass 1c calculateWayEdges. Time elapsed: " + (System.currentTimeMillis() - startTime) + "ms");
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

		relevantNodeEdgeIDs = new IntArrayList[relevantNodeCount];
		//		relevantNodeEdgeTargets = new IntArrayList[relevantNodeCount];
		for (int iNode = 0; iNode < relevantNodeCount; iNode++) {
			relevantNodeEdgeIDs[iNode] = new IntArrayList(1);
			//			relevantNodeEdgeTargets[iNode] = new IntArrayList();

			if ((iNode % 1000000) == 0) {
				System.out.println(String.format("relevantNodeCount create lists %.2f", (double) iNode / relevantNodeCount * 100) + "%");
			}
		}

		for (int iEdge = 0; iEdge < edgeCount; iEdge++) {
			int edgeSource = wayEdgeNodes0[iEdge];
			//			int edgeTarget = wayEdgeNodes1[iEdge];

			//			if (!relevantNodeEdgeTargets[edgeSource].contains(edgeTarget)) {
			relevantNodeEdgeIDs[edgeSource].add(iEdge);
			//				relevantNodeEdgeTargets[edgeSource].add(edgeTarget);
			//			}
			//			else {
			//				duplicatesEdges++;
			//			}

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
		for (int iNode = 0; iNode < relevantNodeCount; iNode++) {
			IntArrayList edges = relevantNodeEdgeIDs[iNode];
			for (int iEdge = 0; iEdge < edges.size(); iEdge++) {
				int edge = edges.getInt(iEdge);
				int target;
				if (wayEdgeNodes0[edge] == iNode) target = wayEdgeNodes1[edge];
				else target = wayEdgeNodes0[edge];

				if (target == iNode) {
					circleEdges++;
					edges.removeInt(iEdge);
					iEdge--;
				}
				else if (targetsTmp.contains(target)) {
					duplicateEdges++;
					edges.removeInt(iEdge);
					iEdge--;
				}
				else {
					targetsTmp.add(target);
				}
			}
			targetsTmp.clear();
		}

		OsmAppPreprocessor.LOG
				.info("removeRedundantEdges finished, removed " + duplicateEdges + " duplicates and " + circleEdges + " circles");
	}


	private void outputText(String outFile) {
		OsmAppPreprocessor.LOG.info("output starting");

		StringBuilder sb = new StringBuilder();
		try (PrintWriter writer = new PrintWriter(outFile)) {
			for (int iNode = 0; iNode < relevantNodeCount; iNode++) {
				IntArrayList edges = relevantNodeEdgeIDs[iNode];
				//if (edges.size() > 0) {
				sb.append(iNode);
				sb.append(';');
				sb.append(relevantNodeLat[iNode]);
				sb.append(';');
				sb.append(relevantNodeLon[iNode]);
				sb.append('|');

				for (int edge : edges) {
					int otherNode;
					if (wayEdgeNodes0[edge] == iNode) otherNode = wayEdgeNodes1[edge];
					else otherNode = wayEdgeNodes0[edge];
					sb.append(otherNode);
					sb.append(';');
					sb.append(wayEdgeLengths[edge]);
					sb.append('|');
				}

				writer.println(sb.toString());
				sb.setLength(0);
				//}

				if ((iNode % 1000000) == 0) {
					System.out.println(String.format("output %.2f", (double) iNode / relevantNodeCount * 100) + "%");
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

		try (DataOutputStream writer = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(outFile)))) {
			writer.writeInt(relevantNodeCount);

			for (int iNode = 0; iNode < relevantNodeCount; iNode++) {
				IntArrayList edges = relevantNodeEdgeIDs[iNode];
				//if (edges.size() > 0) {
				writer.writeInt(iNode);
				writer.writeDouble(relevantNodeLat[iNode]);
				writer.writeDouble(relevantNodeLon[iNode]);

				writer.writeInt(edges.size());
				for (int edge : edges) {
					int otherNode;
					if (wayEdgeNodes0[edge] == iNode) otherNode = wayEdgeNodes1[edge];
					else otherNode = wayEdgeNodes0[edge];
					writer.writeInt(otherNode);
					writer.writeDouble(wayEdgeLengths[edge]);
				}

				//}

				if ((iNode % 1000000) == 0) {
					System.out.println(String.format("output %.2f", (double) iNode / relevantNodeCount * 100) + "%");
				}
			}
		}
		catch (Exception e) {
			OsmAppPreprocessor.LOG.severe("Failure at main: " + e);
			e.printStackTrace();
		}

		OsmAppPreprocessor.LOG.info("output finished");
	}

	//	private void output(String outFile) {
	//		try (PrintWriter writer = new PrintWriter(outFile)) {
	//			for (int iNode = 0; iNode < relevantNodeCount; iNode++) {
	//				IntArrayList edges = relevantNodeEdgeIDs[iNode];
	//				if (edges.size() > 0) {
	//					writer.println(iNode);
	//					for (int edge : edges) {
	//						int otherNode;
	//						if (wayEdgeNodes0[edge] == iNode) otherNode = wayEdgeNodes1[edge];
	//						else otherNode = wayEdgeNodes0[edge];
	//						writer.println("\t" + otherNode + "\t" + wayEdgeLengths[edge]); // TODO Use time, dist/speed?
	//					}
	//				}
	//			}
	//		}
	//		catch (FileNotFoundException e) {
	//			OsmAppPreprocessor.LOG.severe("Failure at main: " + e);
	//			e.printStackTrace();
	//		}
	//
	//		OsmAppPreprocessor.LOG.info("output finished");
	//	}



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
				maxSpeed = 255;
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
		double dist = getNodeDist(relevantNodeLat[i0], relevantNodeLon[i0], relevantNodeLat[i1], relevantNodeLon[i1]);
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
}
