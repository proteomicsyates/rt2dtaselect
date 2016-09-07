package edu.scripps.yates.rt2dtaselect;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.commons.io.FilenameUtils;
import org.apache.log4j.Logger;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.ChannelSftp.LsEntry;
import com.jcraft.jsch.ChannelSftp.LsEntrySelector;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;

import edu.scripps.yates.dtaselect2mzid.util.MS2Reader;
import edu.scripps.yates.dtaselectparser.DTASelectParser;
import edu.scripps.yates.dtaselectparser.util.DTASelectPSM;
import edu.scripps.yates.utilities.fasta.FastaParser;
import edu.scripps.yates.utilities.files.FileUtils;
import edu.scripps.yates.utilities.remote.RemoteSSHFileReference;

public class Rt2DTASelect {
	private static final Logger log = Logger.getLogger(Rt2DTASelect.class);
	public static final String RT = "_RT";
	private final File inputFolder;
	private final String userName;
	private final String pass;
	private static final String HOST_NAME = "jaina.scripps.edu";

	private final static Map<String, File> correspondingMs2Map = new HashMap<String, File>();

	public Rt2DTASelect(File inputFolder, String userName, String password) {
		this.inputFolder = inputFolder;
		this.userName = userName;
		pass = password;
	}

	public static void main(String[] args) {
		if (args.length < 3) {
			throw new IllegalArgumentException("Usage: java -jar rt2dtaselect.jar inputFolder userName password");
		}
		File inputFolder = new File(args[0]);
		if (!inputFolder.exists()) {
			throw new IllegalArgumentException("Folder '" + inputFolder.getAbsolutePath() + "' doesn't exists");
		}
		if (!inputFolder.isDirectory()) {
			throw new IllegalArgumentException("'" + inputFolder.getAbsolutePath() + "' is not a folder");
		}
		String userName = args[1];
		String password = args[2];
		try {
			Rt2DTASelect rt = new Rt2DTASelect(inputFolder, userName, password);
			rt.run();
			System.exit(0);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}

	private void run() throws IOException {
		File[] ms2Files = getFilesByExtension(inputFolder, "ms2", RT);
		File[] dtaSelectFiles = getFilesByExtension(inputFolder, "txt", RT);
		for (File dtaSelectFile : dtaSelectFiles) {
			boolean error = false;

			DTASelectParser parser = new DTASelectParser(dtaSelectFile);
			final String runPath = parser.getRunPath();
			File ms2File = getCorrespondingMS2File(ms2Files, runPath, userName, pass, true);
			MS2Reader ms2Reader = new MS2Reader(ms2File);
			BufferedWriter bw = null;
			BufferedReader br = null;
			// create the output file for that pair
			final String fileName = inputFolder.getAbsolutePath() + File.separator
					+ FilenameUtils.getBaseName(dtaSelectFile.getAbsolutePath()) + RT + ".txt";
			try {

				log.info("Writting " + FilenameUtils.getName(fileName) + "...");
				FileWriter fw = new FileWriter(fileName);
				bw = new BufferedWriter(fw);
				FileReader fr = new FileReader(dtaSelectFile);
				br = new BufferedReader(fr);
				String line;
				boolean intro = false;
				boolean conclusion = false;
				HashMap<String, Integer> psmHeaderPositions = new HashMap<String, Integer>();

				while ((line = br.readLine()) != null) {

					try {
						if (line.startsWith("DTASelect")) {
							intro = true;
							continue;
						}
						if (line.startsWith("Locus")) {
							intro = false;
							continue;
						}
						if (intro) {
							continue;
						}
						if (conclusion) {
							continue;
						}
						// add RT to the end
						if (line.startsWith("Unique")) {
							String[] splitted = line.split("\t");
							for (int position = 0; position < splitted.length; position++) {
								String header = splitted[position];
								psmHeaderPositions.put(header, position);
							}
							line = line + "\tRT";
							continue;
						}

						String[] elements = line.split("\t");
						// if (elements[1].equals("DTASelectProteins")) {
						if (elements[1].equals("Proteins")) {
							conclusion = true;
							continue;
						}

						// this is the case of a protein
						if (isNumeric(elements[1])) {
							continue;
						} else {
							// this is the case of a psm
							try {
								String psmIdentifier = elements[psmHeaderPositions.get(DTASelectPSM.PSM_ID)];
								String scanNumber = FastaParser.getScanFromPSMIdentifier(psmIdentifier);
								scanNumber += "." + scanNumber + "."
										+ FastaParser.getChargeStateFromPSMIdentifier(psmIdentifier);
								final Double rt = ms2Reader.getSpectrumRTByScan(scanNumber);
								if (rt != null) {
									line += "\t" + rt;
								}
							} catch (NumberFormatException e) {
								error = true;
								throw e;
							}
						}
					} finally {
						bw.write(line + "\n");
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
				error = true;

			} finally {
				if (br != null) {
					br.close();
				}
				if (bw != null) {
					bw.close();
				}
				if (!error) {
					log.info("New DTASelect written: " + FilenameUtils.getName(fileName));
				}

			}
		}
	}

	private boolean isNumeric(String string) {
		try {
			Double.valueOf(string);
			return true;
		} catch (NumberFormatException e) {
			return false;
		}
	}

	public File[] getFilesByExtension(File inputFolder, final String extension, final String notEnding) {

		return inputFolder.listFiles(new java.io.FileFilter() {

			public boolean accept(File pathname) {
				if (FilenameUtils.getExtension(pathname.getAbsolutePath()).equals(extension)) {
					if (!FilenameUtils.getBaseName(pathname.getAbsolutePath()).endsWith(notEnding)) {
						return true;
					}
				}
				return false;
			}
		});

	}

	public File getCorrespondingMS2File(File ms2File, String runPath, String userName, String password)
			throws FileNotFoundException {
		File[] files = new File[1];
		files[0] = ms2File;
		return getCorrespondingMS2File(files, runPath, userName, password, true);
	}

	/**
	 * Search the file name into the run path
	 *
	 * @param ms2Files
	 * @param runPath
	 * @param b
	 * @return
	 * @throws FileNotFoundException
	 */
	public File getCorrespondingMS2File(File[] ms2Files, String runPath, String userName, String password,
			boolean goToServer) throws FileNotFoundException {
		if (correspondingMs2Map.containsKey(runPath)) {
			return correspondingMs2Map.get(runPath);
		}
		for (File ms2File : ms2Files) {
			final String baseName = FilenameUtils.getBaseName(ms2File.getAbsolutePath());
			if (runPath.contains(baseName)) {
				correspondingMs2Map.put(runPath, ms2File);
				return ms2File;
			}
		}
		if (goToServer) {
			File ret = getMs2FileFromServer(ms2Files, runPath, userName, password);
			if (ret != null) {
				correspondingMs2Map.put(runPath, ret);
				return ret;
			}
		}
		log.warn("MS2 file not found corresponding to run path: '" + runPath + "'");
		return null;
	}

	public File getMs2FileFromServer(File[] ms2Files, String runPath, String userName, String password) {

		JSch jsch = new JSch();
		Session session = null;
		try {
			log.debug("Getting ms2 from server using remove location:");
			log.debug(runPath);

			session = jsch.getSession(userName, HOST_NAME, 22);
			session.setConfig("StrictHostKeyChecking", "no");
			session.setPassword(password);
			session.connect();

			Channel channel = session.openChannel("sftp");
			channel.connect();
			ChannelSftp sftpChannel = (ChannelSftp) channel;

			final StringTokenizer stringTokenizer = new StringTokenizer(runPath, "/");
			boolean first = true;
			while (stringTokenizer.hasMoreTokens()) {
				String folder = stringTokenizer.nextToken();
				if (first) {
					folder = "/" + folder;
					first = false;
				}
				log.debug("cd " + folder);
				sftpChannel.cd(folder);
			}
			final Set<LsEntry> remoteMs2Files = new HashSet<LsEntry>();
			LsEntrySelector selector = new LsEntrySelector() {

				public int select(LsEntry entry) {
					final String extension2 = FilenameUtils.getExtension(entry.getFilename());
					if ("ms2".equals(extension2)) {
						remoteMs2Files.add(entry);
					}
					return 0;
				}
			};
			sftpChannel.ls(sftpChannel.pwd(), selector);

			log.debug(remoteMs2Files.size() + " ms2 files detected");

			sftpChannel.exit();
			session.disconnect();
			for (LsEntry lsEntry : remoteMs2Files) {
				final File correspondingMS2File = getCorrespondingMS2File(ms2Files, lsEntry.getFilename(), userName,
						password, false);
				if (correspondingMS2File != null) {
					return correspondingMS2File;
				}
			}
			// if not local ms2 files have been returned, download them
			for (LsEntry lsEntry : remoteMs2Files) {
				RemoteSSHFileReference remoteSSHFileReference = new RemoteSSHFileReference(HOST_NAME, userName,
						password, FilenameUtils.getName(lsEntry.getFilename()), new File(inputFolder.getAbsolutePath()
								+ File.separator + FilenameUtils.getName(lsEntry.getFilename())));
				remoteSSHFileReference.setRemotePath(runPath);
				log.info("Trying to download MS2 from server...");
				final File downloadedMs2File = remoteSSHFileReference.getRemoteFile();
				log.info("MS2 downloaded at : " + downloadedMs2File.getAbsolutePath() + ". File size: "
						+ FileUtils.getDescriptiveSizeFromBytes(downloadedMs2File.length()));
				final File correspondingMS2File = getCorrespondingMS2File(downloadedMs2File, lsEntry.getFilename(),
						userName, password);
				if (correspondingMS2File != null) {
					return correspondingMS2File;
				}
			}
		} catch (JSchException e) {
			e.printStackTrace();
			log.warn(e.getMessage());

		} catch (SftpException e) {
			e.printStackTrace();
			log.warn(e.getMessage());

		} catch (IOException e) {
			e.printStackTrace();
			log.warn(e.getMessage());

		}
		return null;
	}
}