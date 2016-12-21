package jgrunert.osm_routing_app;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
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

import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

/**
 * First processing pass, parses input file, pre filtering, searches edges.
 *
 * @author Jonas Grunert
 *
 */
public class OsmAppPreprocessorPass1 {

	static int maxNodesPerWay = 0;
	static int maxWaysPerNode = 0;

	static int elementsPass1 = 0;
	static int elementsPass2 = 0;

	static int BUFFER_MAX_WAYNODES = 100000000;
	static int BUFFER_MAX_WAYSPERNODE = 20;



	static int totalWayCount = 0;
	static int totalNodeCount = 0;
	static int relevantWayNodeCount = 0;

	static LongSet relevantWays = new LongOpenHashSet();
	static Long2ObjectOpenHashMap<LongArrayList> relevantWayNodes = new Long2ObjectOpenHashMap<LongArrayList>();


	// static List<HighwayInfos> highways = new ArrayList<>();
	// IDs of all relevant waypoint nodes
	// static List<Long> waypointIds = new ArrayList<>();


	public static void main(String[] args) {
		try {
			String outDir = args[0];
			String inFile = args[1];
			doPass(inFile, outDir);
		}
		catch (Exception e) {
			OsmAppPreprocessor.LOG.severe("Failure at main");
			OsmAppPreprocessor.LOG.log(Level.SEVERE, "Exception", e);
		}
	}


	public static void doPass(String inFile, String outDir) throws Exception {
		long startTime = System.currentTimeMillis();

		OsmAppPreprocessor.LOG.info("OSM Preprocessor Pass1.a");



		// Map<Long, Short> waysPerNode = new HashMap<>();

		// List of all highways, Int32-index in this array will later be their
		// Pass 1 - read highways
		{

			OsmAppPreprocessor.LOG.info("Starting Pass1a");

			ObjectOutputStream highwaysTempWriter = new ObjectOutputStream(new BufferedOutputStream(
					new FileOutputStream(outDir + File.separator + "pass1-temp-highways.bin")));

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
						if (access != null
								&& (access.equals("private") || access.equals("no") || access.equals("emergency")
										|| access.equals("agricultural") || access.equals("bus"))) {
							accessOk = false;
						}
						else {
							accessOk = true;
						}

						try {
							if (highway != null && accessOk) {
								HighwayInfos hw = evaluateHighway(highway, maxspeed, sidewalk, oneway);
								if (hw != null) {
									// highwayCsvAllWriter.println(highway + ";"
									// + maxspeed + ";" + sidewalk + ";"
									// + oneway + ";" + hw.getCsvString());
									// Write csv
									// highwayCsvWriter.println(hw.getCsvString());
									// Write binary
									// highwayBinWriter.writeBoolean(hw.Car);
									// highwayBinWriter.writeBoolean(hw.Pedestrian);
									// highwayBinWriter.writeBoolean(hw.Oneway);
									relevantWays.add(entity.getId());

									for (WayNode waynode : way.getWayNodes()) {
										LongArrayList nodeWays = relevantWayNodes.get(waynode.getNodeId());
										if (nodeWays == null) {
											nodeWays = new LongArrayList();
											relevantWayNodes.put(waynode.getNodeId(), nodeWays);
										}
										nodeWays.add(entity.getId());
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
									}

									hw.wayNodeIds = new long[way.getWayNodes().size()];
									for (int i = 0; i < way.getWayNodes().size(); i++) {
										hw.wayNodeIds[i] = way.getWayNodes().get(i).getNodeId();
									}

									highwaysTempWriter.writeObject(hw);

									maxNodesPerWay = Math.max(maxNodesPerWay, way.getWayNodes().size());
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

					elementsPass1++;
					if ((elementsPass1 % 1000000) == 0) {
						System.out.println(String.format("Pass1a Loaded %d elements, %d nodes, %d ways", elementsPass1,
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
				reader = new crosby.binary.osmosis.OsmosisReader(
						new BufferedInputStream(new FileInputStream(new File(inFile))));
			}
			catch (FileNotFoundException e1) {
				e1.printStackTrace();
				highwaysTempWriter.close();
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
					highwaysTempWriter.close();
					return;
				}
			}

			highwaysTempWriter.close();

			// highwayCsvWriter.close();

			OsmAppPreprocessor.LOG.info(String.format("Finished Pass1a part 1/2 - loading Nodes %d/%d Ways %d/%d",
					totalNodeCount, relevantWayNodeCount, totalWayCount, relevantWays.size()));
			OsmAppPreprocessor.LOG.info("Time elapsed: " + (System.currentTimeMillis() - startTime) + "ms");
		}



		// Between pass 1 and 2
		// Sort waypointIds
		OsmAppPreprocessor.LOG.info("Start sorting waypointIds with size " + waypointIds.size());
		Collections.sort(waypointIds);
		OsmAppPreprocessor.LOG.info("Sorted waypointIds");

		// Create sorted waypointIdsSet. It maps old to new indices: newIndex ==
		// waypointIdsSet.indexOf(oldIndex)
		OsmAppPreprocessor.LOG.info("Start creating waypointIdsSet");
		// Find out number of unique waypoints
		int waypointIdCount = 1;
		long lastIndex = waypointIds.get(0);
		for (int i = 1; i < waypointIds.size(); i++) {
			if (waypointIds.get(i) != lastIndex) {
				lastIndex = waypointIds.get(i);
				waypointIdCount++;
			}
		}

		long[] waypointIdsSetArray = new long[waypointIdCount];
		lastIndex = waypointIds.get(0);
		waypointIdsSetArray[0] = lastIndex;
		int waypointIdIndex = 1;
		for (int i = 1; i < waypointIds.size(); i++) {
			if (waypointIds.get(i) != lastIndex) {
				lastIndex = waypointIds.get(i);
				waypointIdsSetArray[waypointIdIndex] = lastIndex;
				waypointIdIndex++;
			}
		}
		OsmAppPreprocessor.LOG.info("Finished creating waypointIdsSet with size " + waypointIdsSetArray.length);
		OsmAppPreprocessor.LOG.info("Time elapsed: " + (System.currentTimeMillis() - startTime) + "ms");

		// Idea: Sort by location? Sort by ways (in next step)? Are ways sorted?

		// Save waypointIdsSet
		OsmAppPreprocessor.LOG.info("Start saving waypointIdsSet");
		ObjectOutputStream waypointIdsWriter = new ObjectOutputStream(
				new BufferedOutputStream(new FileOutputStream(outDir + File.separator + "pass1-waynodeIds.bin")));
		waypointIdsWriter.writeObject(waypointIdsSetArray);
		int percTmp100 = waypointIdsSetArray.length / 100;
		waypointIdsWriter.close();
		OsmAppPreprocessor.LOG.info("Finished saving waypointIdsSet");



		// Evaluate and save waypoint highway relations
		ObjectInputStream highwaysTempReader = new ObjectInputStream(
				new BufferedInputStream(new FileInputStream(outDir + File.separator + "pass1-temp-highways.bin")));

		// List of Lists for each node with indices of all ways he is involved
		// in
		DataOutputStream highwayBinWriter = new DataOutputStream(
				new BufferedOutputStream(new FileOutputStream(outDir + File.separator + "pass1-highways.bin")));
		highwayBinWriter.writeInt(relevantWays.size());

		OsmAppPreprocessor.LOG.info("Start finding waysOfNodes");
		List<List<Integer>> waysOfNodes = new ArrayList<List<Integer>>(waypointIdsSetArray.length);
		for (int i = 0; i < waypointIdsSetArray.length; i++) {
			waysOfNodes.add(new LinkedList<Integer>());
		}
		int percAmnt = relevantWays.size() / 100;
		for (int i = 0; i < relevantWays.size(); i++) {
			HighwayInfos hw = (HighwayInfos) highwaysTempReader.readObject();

			highwayBinWriter.writeByte(hw.InfoBits);
			highwayBinWriter.writeBoolean(hw.Oneway);
			highwayBinWriter.writeByte((byte) hw.MaxSpeed);
			highwayBinWriter.writeInt(hw.wayNodeIds.length);

			for (int iWn = 0; iWn < hw.wayNodeIds.length; iWn++) {
				long wnode = hw.wayNodeIds[iWn];
				int nodeIndex = Arrays.binarySearch(waypointIdsSetArray, wnode);
				if (nodeIndex > 0) {
					highwayBinWriter.writeInt(nodeIndex);
					waysOfNodes.get(nodeIndex).add(i);
				}
				else {
					// Cannot find node with this ID
					highwayBinWriter.writeInt(-1);
				}
			}
			if (i % percAmnt == 0) {
				OsmAppPreprocessor.LOG.info((i / percAmnt) + "% finding waysOfNodes");
			}
		}
		highwayBinWriter.close();
		highwaysTempReader.close();
		OsmAppPreprocessor.LOG.info("Finished finding waysOfNodes");
		OsmAppPreprocessor.LOG.info("Time elapsed: " + (System.currentTimeMillis() - startTime) + "ms");


		// Clean up highways
		OsmAppPreprocessor.LOG.info("Start clean up highways");
		OsmAppPreprocessor.LOG.info("Finished clean up highways");


		// Save waysOfNodes
		OsmAppPreprocessor.LOG.info("Start saving waysOfNodes");
		ObjectOutputStream waysOfNodesWriter = new ObjectOutputStream(
				new BufferedOutputStream(new FileOutputStream(outDir + File.separator + "pass1-waysOfNodes.bin")));
		waysOfNodesWriter.writeInt(waysOfNodes.size());
		percTmp100 = waysOfNodes.size() / 100;
		for (int i = 0; i < waysOfNodes.size(); i++) {
			List<Integer> nodeWays = waysOfNodes.get(i);
			waysOfNodesWriter.writeObject(nodeWays.toArray(new Integer[0]));
			// waysOfNodesWriter.writeInt(nodeWays.size());
			// for(int id : nodeWays) {
			// waysOfNodesWriter.writeInt(id);
			// }
			waysOfNodes.set(i, null);
			if (i % percTmp100 == 0) {
				OsmAppPreprocessor.LOG.info(i / percTmp100 + "% save waysOfNodes");
			}
		}
		waysOfNodesWriter.close();
		OsmAppPreprocessor.LOG.info("Finished saving waysOfNodesWriter");


		// Clean up waysOfNodes
		OsmAppPreprocessor.LOG.info("Start clean up waysOfNodes");
		waysOfNodes.clear();
		waysOfNodes = null;
		OsmAppPreprocessor.LOG.info("Finished clean up waysOfNodes");



		OsmAppPreprocessor.LOG.info("Pass 1 finished");
		OsmAppPreprocessor.LOG.info("Relevant ways: " + relevantWays.size() + ", total ways: " + totalWayCount);
		OsmAppPreprocessor.LOG.info("Relevant waynodes: " + relevantWayNodeCount + ", total nodes: " + totalNodeCount);
		OsmAppPreprocessor.LOG.info("Max nodes per way: " + maxNodesPerWay);
		OsmAppPreprocessor.LOG.info("Max ways per node: " + maxWaysPerNode);

		OsmAppPreprocessor.LOG.info("Finished in " + (System.currentTimeMillis() - startTime) + "ms");
	}



	private static class HighwayInfos implements Serializable {

		private static final long serialVersionUID = -1195125473333000206L;

		/** Info bits, bit0: Pedestrian, bit1: Car **/
		public final byte InfoBits;
		public final boolean Oneway;
		public final short MaxSpeed;
		public long[] wayNodeIds;


		public HighwayInfos(boolean car, boolean pedestrian, boolean oneway, short maxSpeed) {
			byte infoBitsTmp = car ? (byte) 1 : (byte) 0;
			infoBitsTmp = (byte) (infoBitsTmp << 1);
			infoBitsTmp += pedestrian ? (byte) 1 : (byte) 0;
			this.InfoBits = infoBitsTmp;
			this.Oneway = oneway;
			this.MaxSpeed = maxSpeed;
		}


		// public String getCsvString() {
		// return InfoBits + ";" + Oneway + ";" + MaxSpeed + ";";
		// }
	}



	static final int SPEED_WALK = 0;
	static final int SPEED_LIVINGSTREET = 5;
	static final int SPEED_UNLIMITED = 255;

	enum SidwalkMode {
		Yes, No, Unspecified
	};

	private static HighwayInfos evaluateHighway(String highwayTag, String maxspeedTag, String sidewalkTag,
			String onewayTag) {

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

			return new HighwayInfos(true, true, oneway, maxSpeed);
		}
		else if (highwayTag.equals("residential")) {
			// residential road
			if (maxSpeed == null) maxSpeed = 50;
			if (oneway == null) oneway = false;

			// Residential by default with sideway
			return new HighwayInfos(true, (sidewalk != SidwalkMode.No), oneway, maxSpeed);
		}
		else if (highwayTag.equals("service")) {
			// service road
			if (maxSpeed == null) maxSpeed = 30;
			if (oneway == null) oneway = false;

			// Residential by default with sideway
			return new HighwayInfos(true, (sidewalk != SidwalkMode.No), oneway, maxSpeed);
		}
		else if (highwayTag.equals("footway") || highwayTag.equals("path") || highwayTag.equals("steps")
				|| highwayTag.equals("bridleway") || highwayTag.equals("pedestrian")) {
			// footway etc.
			if (maxSpeed == null) maxSpeed = 0;
			if (oneway == null) oneway = false;

			return new HighwayInfos(false, true, oneway, maxSpeed);
		}
		else if (highwayTag.startsWith("primary")) {
			// country road etc
			if (maxSpeed == null) maxSpeed = 100;
			if (oneway == null) oneway = false;

			return new HighwayInfos(true, (sidewalk == SidwalkMode.Yes), oneway, maxSpeed);
		}
		else if (highwayTag.startsWith("secondary") || highwayTag.startsWith("tertiary")) {
			// country road etc
			if (maxSpeed == null) maxSpeed = 100;
			if (oneway == null) oneway = false;

			return new HighwayInfos(true, (sidewalk != SidwalkMode.No), oneway, maxSpeed);
		}
		else if (highwayTag.equals("unclassified")) {
			// unclassified (small road)
			if (maxSpeed == null) maxSpeed = 50;
			if (oneway == null) oneway = false;

			return new HighwayInfos(true, (sidewalk != SidwalkMode.No), oneway, maxSpeed);
		}
		else if (highwayTag.equals("living_street")) {
			// living street
			if (maxSpeed == null) maxSpeed = SPEED_LIVINGSTREET;
			if (oneway == null) oneway = false;

			return new HighwayInfos(true, true, oneway, maxSpeed);
		}
		else if (highwayTag.startsWith("motorway")) {
			// track
			if (maxSpeed == null) maxSpeed = 255;
			if (oneway == null) oneway = true;

			return new HighwayInfos(true, (sidewalk == SidwalkMode.Yes), oneway, maxSpeed);
		}
		else if (highwayTag.startsWith("trunk")) {
			// trunk road
			if (maxSpeed == null) maxSpeed = 255;
			if (oneway == null) oneway = false;

			return new HighwayInfos(true, (sidewalk == SidwalkMode.Yes), oneway, maxSpeed);
		}

		// Ignore this road if no useful classification available
		return null;
	}
}
