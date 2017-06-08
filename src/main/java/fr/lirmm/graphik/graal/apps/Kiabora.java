package fr.lirmm.graphik.graal.apps;

import java.io.FileInputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang3.StringUtils;
import org.jgrapht.graph.DefaultEdge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import fr.lirmm.graphik.graal.api.core.Rule;
import fr.lirmm.graphik.graal.api.core.RuleSetException;
import fr.lirmm.graphik.graal.core.stream.filter.RuleFilterIterator;
import fr.lirmm.graphik.graal.core.unifier.checker.ProductivityChecker;
import fr.lirmm.graphik.graal.core.unifier.checker.RestrictedProductivityChecker;
import fr.lirmm.graphik.graal.io.dlp.DlgpParser;
import fr.lirmm.graphik.graal.io.dlp.DlgpWriter;
import fr.lirmm.graphik.graal.rulesetanalyser.Analyser;
import fr.lirmm.graphik.graal.rulesetanalyser.RuleSetPropertyHierarchy;
import fr.lirmm.graphik.graal.rulesetanalyser.graph.GraphPositionDependencies;
import fr.lirmm.graphik.graal.rulesetanalyser.graph.GraphPositionDependencies.SpecialEdge;
import fr.lirmm.graphik.graal.rulesetanalyser.property.RuleSetProperty;
import fr.lirmm.graphik.graal.rulesetanalyser.util.AnalyserRuleSet;
import fr.lirmm.graphik.util.Apps;
import fr.lirmm.graphik.util.graph.scc.StronglyConnectedComponentsGraph;

/*
 * What remains:
 *   - first, we should implement some tests to check if everything
 *   works correctly;
 *   - second, we should implement another main that will do
 *   conversions that old kiabora (may it rest in peace) did; => DONE
 *   - finally, upgrade the servlet so it calls this program.
 */

/**
 * Analyse a rule set.
 * 
 * The input file must be DLGP formatted.
 * 
 * For details about the various arguments use '--help'.
 */
public class Kiabora {

	private static final Logger LOGGER = LoggerFactory.getLogger(Kiabora.class);

	public static final String PROGRAM_NAME = "kiabora";
	public static final Map<String, RuleSetProperty> propertyMap = RuleSetPropertyHierarchy.generatePropertyMap();

	public static void main(String args[]) {
		Kiabora options = new Kiabora();

		JCommander commander = null;
		try {
			commander = new JCommander(options, args);
		} catch (com.beust.jcommander.ParameterException e) {
			System.err.println(e.getMessage());
			System.exit(1);
		}

		if (options.help) {
			Apps.printVersion(PROGRAM_NAME);
			System.out.println("For more details about Kiabora: see http://graphik-team.github.io/graal/kiabora");
			commander.usage();
			System.exit(0);
		}

		if (options.version) {
			Apps.printVersion(PROGRAM_NAME);
			System.exit(0);
		}

		if (options.list_properties) {
			printPropertiesList(propertyMap);
			System.exit(0);
		}

		if (options.alias) {
			List<String> newArgs = new ArrayList<String>();
			for (String s : args) {
				newArgs.add(s);
			}
			newArgs.add("-c");
			newArgs.add("-b");
			newArgs.add("-g");
			newArgs.add("-p");
			newArgs.add("*");
			newArgs.add("-P");
			newArgs.add("-r");
			newArgs.add("-R");
			newArgs.add("-s");
			newArgs.add("-G");
			newArgs.add("-S");
			newArgs.add("-u");
			try {
				commander = new JCommander(options, newArgs.toArray(new String[newArgs.size()]));
			} catch (com.beust.jcommander.ParameterException e) {
				System.err.println(e.getMessage());
				System.exit(1);
			}
		}

		// init parser
		DlgpParser parser = null;

		if (options.input_filepath.equals("-"))
			parser = new DlgpParser(System.in);
		else {
			try {
				parser = new DlgpParser(new FileInputStream(options.input_filepath));
			} catch (Exception e) {
				System.err.println("Could not open file: " + options.input_filepath);
				System.err.println(e);
				e.printStackTrace();
				System.exit(1);
			}
		}

		// parse rule set
		AnalyserRuleSet ruleset = null;
		try {
			ruleset = new AnalyserRuleSet(new RuleFilterIterator(parser));
		} catch (RuleSetException e) {
			System.err.println("An error occured when parsing rules: " + e.getMessage());
			System.exit(1);
		}

		if (options.with_unifiers)
			ruleset.enableUnifiers(true);

		if (options.restricted_grd) {
			ruleset.removeDependencyChecker(ProductivityChecker.instance());
			ruleset.addDependencyChecker(RestrictedProductivityChecker.instance());
		}
		

		// set up analyser
		Map<String, RuleSetProperty> properties = new TreeMap<String, RuleSetProperty>();
		for (String label : options.ruleset_properties) {
			if (label.equals("*"))
				properties.putAll(propertyMap);
			else {
				if (propertyMap.get(label) != null)
					properties.put(label, propertyMap.get(label));
				else if (LOGGER.isWarnEnabled())
					LOGGER.warn("Requesting unknown property: " + label);
			}
		}
		RuleSetPropertyHierarchy hierarchy = new RuleSetPropertyHierarchy(properties.values());

		Analyser analyser = new Analyser();
		analyser.setProperties(hierarchy);
		analyser.setRuleSet(ruleset);


		if (options.print_ruleset) {
			System.out.println("====== RULE SET ======");
			printRuleSet(ruleset);
			System.out.println("");
		}

		if (options.print_grd) {
			System.out.println("======== GRD =========");
			printGRD(ruleset);
			System.out.println("");
		}

		if (options.print_scc) {
			System.out.println("======== SCC =========");
			printSCC(ruleset);
			System.out.println("");
		}

		if (options.print_sccg) {
			System.out.println("===== SCC GRAPH ======");
			printSCCGraph(ruleset);
			System.out.println("");
		}

		if (options.print_ppg) {
			System.out.println("===== PP GRAPH ======");
			printPPGraph(ruleset);
			System.out.println("");
		}

		if (options.print_rule_pties) {
			System.out.println("== RULE PROPERTIES ===");
			printRuleProperties(analyser);
			System.out.println("");
		}

		if (options.print_pties) {
			System.out.println("===== PROPERTIES =====");
			printProperties(analyser);
			System.out.println("");
		}

		if (options.print_scc_pties) {
			System.out.println("=== SCC PROPERTIES ===");
			printSCCProperties(analyser);
			System.out.println("");
		}
		
		System.out.print("===== ANALYSIS: ");
		if(analyser.isDecidable()) {
			System.out.println("DECIDABLE =====");
		} else {
			System.out.println("no proof of decidability found =====");
		}
		System.out.println("");


		if (options.combine_fes) {
			System.out.println("=== COMBINE (FES) ====");
			printCombineFES(analyser);
			System.out.println("");
		}

		if (options.combine_fus) {
			System.out.println("=== COMBINE (FUS) ====");
			printCombineFUS(analyser);
			System.out.println("");
		}

	}

	public static void printRuleSet(AnalyserRuleSet ruleset) {
		for (Rule r : ruleset) {
			System.out.print(DlgpWriter.writeToString(r));
		}
	}

	public static void printGRD(AnalyserRuleSet ruleset) {
		System.out.println(ruleset.getGraphOfRuleDependencies().toString());
	}

	public static void printSCC(AnalyserRuleSet ruleset) {
		StringBuilder out = new StringBuilder();
		StronglyConnectedComponentsGraph<Rule> scc = ruleset.getStronglyConnectedComponentsGraph();
		boolean first;
		for (int v : scc.vertexSet()) {
			out.append("C" + v + " = {");
			first = true;
			for (Rule r : scc.getComponent(v)) {
				if (first) {
					first = false;
				} else {
					out.append(", ");
				}
				out.append(r.getLabel());
			}
			out.append("}\n");
		}
		System.out.println(out);
	}

	public static void printSCCGraph(AnalyserRuleSet ruleset) {
		StringBuilder out = new StringBuilder();
		StronglyConnectedComponentsGraph<Rule> scc = ruleset.getStronglyConnectedComponentsGraph();
		boolean first;
		for (int v : scc.vertexSet()) {
			out.append("C" + v);
			first = true;
			for (int t : scc.outgoingEdgesOf(v)) {
				if (first) {
					first = false;
					out.append(" ---> ");
				} else
					out.append(", ");
				out.append("C" + scc.getEdgeTarget(t));
			}
			out.append("\n");
		}
		System.out.println(out);
	}
	
	/**
	 * @param out
	 * @param onto
	 */
	private static void printPPGraph(AnalyserRuleSet onto) {
		PrintStream out = System.out;
		GraphPositionDependencies ppg = onto.getGraphPositionDependencies();
		for (DefaultEdge e : ppg.edgeSet()) {
			out.print(ppg.getEdgeSource(e));
			if (e instanceof SpecialEdge) {
				out.print(" ~> ");
			} else {
				out.print(" -> ");
			}
			out.print(ppg.getEdgeTarget(e));
		}
	}

	public static void printRuleProperties(Analyser analyser) {
		int cell_size = 6;
		StringBuilder out = new StringBuilder();
		Map<String, Integer> basePties = analyser.ruleProperties().iterator().next();
		Iterator<Rule> rules = analyser.getRuleSet().iterator();

		if (basePties == null)
			return;

		out.append("+");
		out.append(StringUtils.center("", (cell_size + 1) * basePties.entrySet().size() - 1, '-'));
		out.append("+");
		out.append("\n");

		for (Map<String, Integer> pties : analyser.ruleProperties()) {
			for (Map.Entry<String, Integer> e : pties.entrySet()) {
				out.append("|");
				if (e.getValue() == 0)
					out.append(StringUtils.center("?", cell_size));
				else if (e.getValue() < 0)
					out.append(StringUtils.center("-", cell_size));
				else
					out.append(StringUtils.center("X", cell_size));
			}
			out.append("|");
			out.append(StringUtils.center(rules.next().getLabel(), cell_size));
			out.append("\n");
		}

		out.append("+");
		out.append(StringUtils.center("", (cell_size + 1) * basePties.entrySet().size() - 1, '-'));
		out.append("+\n");
		for (Map.Entry<String, Integer> e : basePties.entrySet()) {
			out.append("|");
			out.append(StringUtils.center(e.getKey(), cell_size));
		}
		out.append("|\n");
		out.append("+");
		out.append(StringUtils.center("", (cell_size + 1) * basePties.entrySet().size() - 1, '-'));
		out.append("+");

		System.out.println(out);
	}

	public static void printProperties(Analyser analyser) {
		int cell_size = 6;
		StringBuilder out = new StringBuilder();
		Map<String, Integer> pties = analyser.ruleSetProperties();

		out.append("+");
		out.append(StringUtils.center("", (cell_size + 1) * pties.entrySet().size() - 1, '-'));
		out.append("+");
		out.append("\n");
		for (Map.Entry<String, Integer> e : pties.entrySet()) {
			out.append("|");
			if (e.getValue() == 0)
				out.append(StringUtils.center("?", cell_size));
			else if (e.getValue() < 0)
				out.append(StringUtils.center("-", cell_size));
			else
				out.append(StringUtils.center("X", cell_size));
		}
		out.append("|\n");
		out.append("+");
		out.append(StringUtils.center("", (cell_size + 1) * pties.entrySet().size() - 1, '-'));
		out.append("+\n");
		for (Map.Entry<String, Integer> e : pties.entrySet()) {
			out.append("|");
			out.append(StringUtils.center(e.getKey(), cell_size));
		}
		out.append("|\n");
		out.append("+");
		out.append(StringUtils.center("", (cell_size + 1) * pties.entrySet().size() - 1, '-'));
		out.append("+");
		System.out.println(out);
	}

	public static void printSCCProperties(Analyser analyser) {
		int cell_size = 6;
		StringBuilder out = new StringBuilder();
		Map<String, Integer> basePties = analyser.sccProperties().iterator().next();

		int cIndex = 0;
		if (basePties == null)
			return;

		out.append("+");
		out.append(StringUtils.center("", (cell_size + 1) * basePties.entrySet().size() - 1, '-'));
		out.append("+");
		out.append("\n");

		for (Map<String, Integer> pties : analyser.sccProperties()) {
			for (Map.Entry<String, Integer> e : pties.entrySet()) {
				out.append("|");
				if (e.getValue() == 0)
					out.append(StringUtils.center("?", cell_size));
				else if (e.getValue() < 0)
					out.append(StringUtils.center("-", cell_size));
				else
					out.append(StringUtils.center("X", cell_size));
			}
			out.append("|");
			out.append(StringUtils.center("C" + cIndex++, cell_size));
			out.append("\n");
		}

		out.append("+");
		out.append(StringUtils.center("", (cell_size + 1) * basePties.entrySet().size() - 1, '-'));
		out.append("+\n");
		for (Map.Entry<String, Integer> e : basePties.entrySet()) {
			out.append("|");
			out.append(StringUtils.center(e.getKey(), cell_size));
		}
		out.append("|\n");
		out.append("+");
		out.append(StringUtils.center("", (cell_size + 1) * basePties.entrySet().size() - 1, '-'));
		out.append("+");

		System.out.println(out);
	}

	public static void printCombineFES(Analyser analyser) {
		int combine[] = analyser.combineFES();
		if (combine == null) {
			System.out.println("None!");
			return;
		}

		StringBuilder out = new StringBuilder();
		for (int i = 0; i < combine.length; ++i) {
			out.append("C" + i + ": ");
			if ((combine[i] & Analyser.COMBINE_FES) != 0)
				out.append("FES");
			else if ((combine[i] & Analyser.COMBINE_FUS) != 0)
				out.append("FUS");
			else if ((combine[i] & Analyser.COMBINE_BTS) != 0)
				out.append("BTS");
			out.append("\n");
		}

		System.out.println(out);
	}

	public static void printCombineFUS(Analyser analyser) {
		int combine[] = analyser.combineFUS();
		if (combine == null) {
			System.out.println("None!");
			return;
		}

		StringBuilder out = new StringBuilder();
		for (int i = 0; i < combine.length; ++i) {
			out.append("C" + i + ": ");
			if ((combine[i] & Analyser.COMBINE_FES) != 0)
				out.append("FES");
			else if ((combine[i] & Analyser.COMBINE_FUS) != 0)
				out.append("FUS");
			else if ((combine[i] & Analyser.COMBINE_BTS) != 0)
				out.append("BTS");
			out.append("\n");
		}

		System.out.println(out);
	}

	public static void printPropertiesList(Map<String, RuleSetProperty> properties) {
		for (RuleSetProperty p : properties.values()) {
			System.out.println(p.getLabel() + ": \t" + p.getFullName() + " - " + p.getDescription());
		}
	}

	@Parameter(names = { "-f", "--input-file" },
	           description = "Rule set input file (use '-' for stdin).")
	private String input_filepath = "-";

	@Parameter(names = { "-p", "--properties" },
	           description = "Select which properties must be checked (example: 'lin,agrd,s,fus' or '*' to select all)."
	             + " See --list-properties for a list of available properties.",
	           variableArity = true)
	private List<String> ruleset_properties = new LinkedList<String>(propertyMap.keySet());

	@Parameter(names = { "-l", "--list-properties" }, 
			   description = "Print the available rule set properties.")
	private boolean list_properties = false;

	@Parameter(names = { "-g", "--grd" },
	           description = "Print the Graph of Rule Dependencies.")
	private boolean print_grd = false;

	@Parameter(names = { "-s", "--scc" },
	           description = "Print the GRD Strongly Connected Components.")
	private boolean print_scc = false;

	@Parameter(names = { "-G", "--scc-graph" },
	           description = "Print the graph of the GRD Strongly Connected Components.")
	private boolean print_sccg = false;
	
	@Parameter(names = { "--ppg-graph" },
	           description = "Print the predicate position graph.")
	private boolean print_ppg = false;

	@Parameter(names = { "-r", "--rule-set" },
	           description = "Print the rule set (can be useful if some rules were not labelled in the input file).")
	private boolean print_ruleset = false;

	@Parameter(names = { "-P", "--rule-properties" }, 
	           description = "Print properties for each rule.")
	private boolean print_rule_pties = false;

	@Parameter(names = { "-S", "--scc-properties" },
	           description = "Print properties for each GRD SCC.")
	private boolean print_scc_pties = false;

	@Parameter(names = { "-R", "--ruleset-properties" },
	           description = "Print properties for the whole rule set.")
	private boolean print_pties = false;

	@Parameter(names = { "-c", "--combine-fes" },
	           description = "Combine GRD connected components in attempt to find some decidable combination while maximizing the forward chaining (chase).")
	private boolean combine_fes = false;

	@Parameter(names = { "-b", "--combine-fus" },
	           description = "Combine GRD connected components in attempt to find some decidable combination while maximizing the backward chaining (query rewriting).")
	private boolean combine_fus = false;

	@Parameter(names = { "-u", "--unifiers" },
	           description = "Compute all unifiers between rules in order to print them in the GRD.")
	private boolean with_unifiers = false;
	
	@Parameter(names = { "--restricted-grd"}, description = "filter some dependencies in the GRD that is not productive in a restricted chase algorithm.")
	private boolean restricted_grd = false;

	@Parameter(names = { "-h", "--help" },
	           description = "Print this message.")
	private boolean help = false;

	@Parameter(names = { "-V", "--version" }, description = "Print version information")
	private boolean version = false;

	@Parameter(names = { "-a" }, description = "Alias for -c -b -g -p '*' -P -r -R -s -G -S -u.")
	private boolean alias = false;

};

