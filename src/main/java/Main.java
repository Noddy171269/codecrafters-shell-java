import java.util.*;
import java.io.*;
import java.nio.file.*;
import java.util.concurrent.TimeUnit;

public class Main {

    static File current = new File(System.getProperty("user.dir"));
    static ArrayList<Job> jobs = new ArrayList<>();

    static class Job {
        int id;
        long pid;
        String cmd;
        Process process;

        Job(int id, long pid, String cmd, Process process) {
            this.id = id;
            this.pid = pid;
            this.cmd = cmd;
            this.process = process;
        }
    }

    public static void main(String[] args) throws Exception {

        Scanner sc = new Scanner(System.in);

        while (true) {

            reapJobs();

            System.out.print("$ ");
            System.out.flush();

            if (!sc.hasNextLine())
                break;

            String line = sc.nextLine();

            List<String> t = parse(line);

            if (t.isEmpty())
                continue;

            boolean bg = false;

            if (t.get(t.size()-1).equals("&")) {
                bg = true;
                t = new ArrayList<>(t.subList(0, t.size()-1));
            }

            List<List<String>> pipelineSegments = splitByPipe(t);

            if (pipelineSegments.size() > 1) {

                List<String> lastSeg = new ArrayList<>(pipelineSegments.get(pipelineSegments.size()-1));

                String pOut = null, pErr = null;
                boolean pOutAppend = false, pErrAppend = false;

                // Extract redirections from last segment
                List<String> cleanedLastSeg = new ArrayList<>();
                for (int i = 0; i < lastSeg.size(); i++) {
                    String x = lastSeg.get(i);
                    if ((x.equals(">") || x.equals("1>")) && i+1 < lastSeg.size()) {
                        pOut = lastSeg.get(i+1);
                        pOutAppend = false;
                        i++;
                    } else if ((x.equals(">>") || x.equals("1>>")) && i+1 < lastSeg.size()) {
                        pOut = lastSeg.get(i+1);
                        pOutAppend = true;
                        i++;
                    } else if (x.equals("2>") && i+1 < lastSeg.size()) {
                        pErr = lastSeg.get(i+1);
                        pErrAppend = false;
                        i++;
                    } else if (x.equals("2>>") && i+1 < lastSeg.size()) {
                        pErr = lastSeg.get(i+1);
                        pErrAppend = true;
                        i++;
                    } else {
                        cleanedLastSeg.add(x);
                    }
                }

                pipelineSegments.set(pipelineSegments.size()-1, cleanedLastSeg);

                runPipeline(pipelineSegments, pOut, pOutAppend, pErr, pErrAppend, bg);

                continue;
            }

            // Single command — extract all redirections
            String out = null, err = null;
            boolean outAppend = false, errAppend = false;

            List<String> cleaned = new ArrayList<>();
            for (int i = 0; i < t.size(); i++) {
                String x = t.get(i);
                if ((x.equals(">") || x.equals("1>")) && i+1 < t.size()) {
                    out = t.get(i+1);
                    outAppend = false;
                    i++;
                } else if ((x.equals(">>") || x.equals("1>>")) && i+1 < t.size()) {
                    out = t.get(i+1);
                    outAppend = true;
                    i++;
                } else if (x.equals("2>") && i+1 < t.size()) {
                    err = t.get(i+1);
                    errAppend = false;
                    i++;
                } else if (x.equals("2>>") && i+1 < t.size()) {
                    err = t.get(i+1);
                    errAppend = true;
                    i++;
                } else {
                    cleaned.add(x);
                }
            }
            t = cleaned;

            if (t.isEmpty())
                continue;

            String cmd = t.get(0);

            if (cmd.equals("exit")) {
                int code = 0;
                if (t.size() > 1) {
                    try {
                        code = Integer.parseInt(t.get(1));
                    } catch (NumberFormatException e) {
                        // default 0
                    }
                }
                System.exit(code);
            }

            else if (cmd.equals("jobs")) {

                int n = jobs.size();

                StringBuilder jobsOut = new StringBuilder();

                ArrayList<Job> finished = new ArrayList<>();

                for (int i = 0; i < n; i++) {

                    Job j = jobs.get(i);

                    String mark =
                            (i==n-1) ? "+" :
                            (i==n-2) ? "-" :
                            " ";

                    if (isFinished(j.process)) {

                        jobsOut.append(String.format(
                                "[%d]%s  Done                    %s%n",
                                j.id,
                                mark,
                                j.cmd
                        ));

                        finished.add(j);

                    } else {

                        jobsOut.append(String.format(
                                "[%d]%s  Running                 %s &%n",
                                j.id,
                                mark,
                                j.cmd
                        ));

                    }

                }

                jobs.removeAll(finished);

                if (out != null) {

                    writeToFile(out, jobsOut.toString(), outAppend);

                } else {

                    System.out.print(jobsOut);

                }

                ensureFile(err, errAppend);

            }

            else if (cmd.equals("pwd")) {

                print(
                        current.getCanonicalPath(),
                        out,
                        outAppend
                );

                ensureFile(err, errAppend);

            }

            else if (cmd.equals("cd")) {

                String p = t.size() > 1 ? t.get(1) : System.getenv("HOME");

                File d;

                if (p.equals("~"))

                    d = new File(System.getenv("HOME"));

                else if (p.startsWith("/"))

                    d = new File(p);

                else

                    d = new File(current, p);

                if (d.exists() && d.isDirectory())

                    current = d.getCanonicalFile();

                else

                    printErr(
                            "cd: "+p+": No such file or directory",
                            err,
                            errAppend
                    );

                ensureFile(out, outAppend);

            }

            else if (cmd.equals("echo")) {

                print(
                        String.join(" ", t.subList(1, t.size())),
                        out,
                        outAppend
                );

                ensureFile(err, errAppend);

            }

            else if (cmd.equals("type")) {

                String c = t.get(1);

                if (isBuiltin(c)) {

                    print(
                            c+" is a shell builtin",
                            out,
                            outAppend
                    );

                } else {

                    String f = find(c);

                    if (f != null)

                        print(
                                c+" is "+f,
                                out,
                                outAppend
                        );

                    else

                        print(
                                c+": not found",
                                out,
                                outAppend
                        );

                }

                ensureFile(err, errAppend);

            }

            else {

                String exe = find(cmd);

                if (exe == null) {

                    printErr(
                            cmd+": command not found",
                            err,
                            errAppend
                    );

                    continue;
                }

                ProcessBuilder pb = new ProcessBuilder(t);

                pb.directory(current);

                if (out != null) {

                    if (outAppend)

                        pb.redirectOutput(
                                ProcessBuilder.Redirect.appendTo(
                                        resolveFile(out)));

                    else

                        pb.redirectOutput(
                                resolveFile(out));

                } else {

                    pb.redirectOutput(
                            ProcessBuilder.Redirect.INHERIT);

                }

                if (err != null) {

                    if (errAppend)

                        pb.redirectError(
                                ProcessBuilder.Redirect.appendTo(
                                        resolveFile(err)));

                    else

                        pb.redirectError(
                                resolveFile(err));

                } else {

                    pb.redirectError(
                            ProcessBuilder.Redirect.INHERIT);

                }

                Process p = pb.start();

                if (bg) {

                    Job j = new Job(
                            nextJobId(),
                            p.pid(),
                            String.join(" ", t),
                            p
                    );

                    jobs.add(j);

                    System.out.println(
                            "["+j.id+"] "+j.pid
                    );

                } else {

                    p.waitFor();

                }

            }

        }

    }

    // -------------------------------------------------------------------------
    // Pipeline execution — supports both external commands and built-ins.
    // -------------------------------------------------------------------------

    static void runPipeline(List<List<String>> segments,
                            String out, boolean outAppend,
                            String err, boolean errAppend,
                            boolean bg) throws Exception {

        int n = segments.size();

        // Decide the final stdout destination once.
        OutputStream finalOut;
        if (out != null) {
            finalOut = outAppend
                    ? new FileOutputStream(resolveFile(out), true)
                    : new FileOutputStream(resolveFile(out), false);
        } else {
            finalOut = System.out;
        }

        // Build inter-stage pipes
        PipedInputStream[]  pipeIn  = new PipedInputStream [n-1];
        PipedOutputStream[] pipeOut = new PipedOutputStream[n-1];
        for (int i = 0; i < n-1; i++) {
            pipeOut[i] = new PipedOutputStream();
            pipeIn[i]  = new PipedInputStream(pipeOut[i], 65536);
        }

        List<Thread>  threads        = new ArrayList<>();
        List<Process> processes      = new ArrayList<>();
        Thread        lastBuiltinTh  = null;

        for (int i = 0; i < n; i++) {

            List<String> seg  = segments.get(i);
            String       cmd0 = seg.get(0);

            final InputStream  stageIn  = (i == 0)   ? System.in      : pipeIn[i-1];
            final OutputStream stageOut = (i == n-1) ? finalOut        : pipeOut[i];
            final boolean      isLast   = (i == n-1);

            if (isBuiltin(cmd0)) {

                final List<String> fseg = seg;

                Thread th = new Thread(() -> {
                    try {
                        runBuiltinInPipeline(fseg, stageIn, stageOut, err, errAppend, isLast);
                    } catch (Exception e) {
                        // ignore
                    } finally {
                        boolean isSystemOut = (stageOut == System.out);
                        if (!isSystemOut) {
                            try { stageOut.close(); } catch (Exception ignore) {}
                        } else {
                            try { System.out.flush(); } catch (Exception ignore) {}
                        }
                        if (stageIn != System.in) {
                            try { stageIn.close(); } catch (Exception ignore) {}
                        }
                    }
                });

                th.setDaemon(false);
                th.start();
                if (isLast) lastBuiltinTh = th;
                else threads.add(th);

            } else {

                String exe = find(cmd0);

                if (exe == null) {
                    System.err.println(cmd0 + ": command not found");
                    for (int k = 0; k < n-1; k++) {
                        try { pipeOut[k].close(); } catch (Exception ignore) {}
                        try { pipeIn[k].close();  } catch (Exception ignore) {}
                    }
                    if (finalOut != System.out) finalOut.close();
                    return;
                }

                ProcessBuilder pb = new ProcessBuilder(seg);
                pb.directory(current);

                // stdin
                if (i == 0) {
                    pb.redirectInput(ProcessBuilder.Redirect.INHERIT);
                } else {
                    pb.redirectInput(ProcessBuilder.Redirect.PIPE);
                }

                // stdout
                if (isLast) {
                    if (out != null) {
                        if (outAppend)
                            pb.redirectOutput(ProcessBuilder.Redirect.appendTo(resolveFile(out)));
                        else
                            pb.redirectOutput(resolveFile(out));
                    } else {
                        pb.redirectOutput(ProcessBuilder.Redirect.INHERIT);
                    }
                } else {
                    pb.redirectOutput(ProcessBuilder.Redirect.PIPE);
                }

                // stderr
                if (isLast && err != null) {
                    if (errAppend)
                        pb.redirectError(ProcessBuilder.Redirect.appendTo(resolveFile(err)));
                    else
                        pb.redirectError(resolveFile(err));
                } else {
                    pb.redirectError(ProcessBuilder.Redirect.INHERIT);
                }

                Process proc = pb.start();
                processes.add(proc);

                // Pump pipe -> process stdin
                if (i > 0) {
                    final InputStream  src  = stageIn;
                    final OutputStream dst  = proc.getOutputStream();
                    Thread pump = new Thread(() -> {
                        try {
                            byte[] buf = new byte[8192];
                            int nRead;
                            while ((nRead = src.read(buf)) != -1) {
                                dst.write(buf, 0, nRead);
                                dst.flush();
                            }
                        }
                        catch (Exception ignore) {}
                        finally {
                            try { dst.close(); } catch (Exception ignore2) {}
                            try { src.close(); } catch (Exception ignore2) {}
                        }
                    });
                    pump.setDaemon(true);
                    pump.start();
                    threads.add(pump);
                }

                // Pump process stdout -> pipe
                if (!isLast) {
                    final InputStream  src  = proc.getInputStream();
                    final OutputStream dst  = stageOut;
                    Thread pump = new Thread(() -> {
                        try {
                            byte[] buf = new byte[8192];
                            int nRead;
                            while ((nRead = src.read(buf)) != -1) {
                                dst.write(buf, 0, nRead);
                                dst.flush();
                            }
                        }
                        catch (Exception ignore) {}
                        finally {
                            try { dst.close(); } catch (Exception ignore2) {}
                            try { src.close(); } catch (Exception ignore2) {}
                        }
                    });
                    pump.setDaemon(true);
                    pump.start();
                    threads.add(pump);
                }
            }
        }

        // Wait for everything.
        if (bg) {
            if (!processes.isEmpty()) {
                Process lastProc = processes.get(processes.size()-1);
                StringBuilder cmdStr = new StringBuilder();
                for (int i = 0; i < segments.size(); i++) {
                    if (i > 0) cmdStr.append(" | ");
                    cmdStr.append(String.join(" ", segments.get(i)));
                }
                Job j = new Job(nextJobId(), lastProc.pid(), cmdStr.toString(), lastProc);
                jobs.add(j);
                System.out.println("[" + j.id + "] " + j.pid);
            }
        } else {
            if (lastBuiltinTh != null) {
                lastBuiltinTh.join();
                for (Process p : processes) {
                    if (p.isAlive()) p.destroyForcibly();
                }
                for (Process p : processes) p.waitFor();
            } else if (!processes.isEmpty()) {
                processes.get(processes.size()-1).waitFor();
                for (int i = 0; i < processes.size()-1; i++) {
                    Process p = processes.get(i);
                    if (p.isAlive()) p.destroyForcibly();
                }
                for (Process p : processes) p.waitFor();
            }
            for (Thread th : threads) th.join(2000);
            System.out.flush();
            if (finalOut != System.out) finalOut.close();
        }
    }

    // -------------------------------------------------------------------------
    // Execute a single built-in command inside a pipeline.
    // -------------------------------------------------------------------------
    static void runBuiltinInPipeline(List<String> seg,
                                     InputStream in,
                                     OutputStream out,
                                     String errFile, boolean errAppend,
                                     boolean isLast) throws Exception {

        String cmd = seg.get(0);
        PrintStream ps = (out == System.out)
                ? System.out
                : new PrintStream(out, /*autoFlush=*/true);

        switch (cmd) {

            case "echo": {
                ps.println(String.join(" ", seg.subList(1, seg.size())));
                break;
            }

            case "pwd": {
                ps.println(current.getCanonicalPath());
                break;
            }

            case "type": {
                String c = seg.get(1);
                if (isBuiltin(c)) {
                    ps.println(c + " is a shell builtin");
                } else {
                    String f = find(c);
                    if (f != null)
                        ps.println(c + " is " + f);
                    else
                        ps.println(c + ": not found");
                }
                break;
            }

            case "cd": {
                String p = seg.get(1);
                File d;
                if (p.equals("~"))
                    d = new File(System.getenv("HOME"));
                else if (p.startsWith("/"))
                    d = new File(p);
                else
                    d = new File(current, p);
                if (d.exists() && d.isDirectory())
                    current = d.getCanonicalFile();
                else
                    System.err.println("cd: " + p + ": No such file or directory");
                break;
            }

            case "jobs": {
                int n = jobs.size();
                ArrayList<Job> finished = new ArrayList<>();
                for (int i = 0; i < n; i++) {
                    Job j = jobs.get(i);
                    String mark = (i==n-1) ? "+" : (i==n-2) ? "-" : " ";
                    if (isFinished(j.process)) {
                        ps.printf("[%d]%s  Done                    %s%n", j.id, mark, j.cmd);
                        finished.add(j);
                    } else {
                        ps.printf("[%d]%s  Running                 %s &%n", j.id, mark, j.cmd);
                    }
                }
                jobs.removeAll(finished);
                break;
            }

            // "exit" inside a pipeline is ignored
            default:
                break;
        }

        ps.flush();

        if (isLast) ensureFile(errFile, errAppend);
    }

    static List<List<String>> splitByPipe(List<String> t) {

        List<List<String>> segs = new ArrayList<>();
        List<String> cur = new ArrayList<>();

        for (String x : t) {
            if (x.equals("|")) {
                segs.add(cur);
                cur = new ArrayList<>();
            } else {
                cur.add(x);
            }
        }

        segs.add(cur);
        return segs;
    }

    static boolean isFinished(Process p) {
        if (!p.isAlive()) return true;
        try {
            return p.waitFor(50, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            return !p.isAlive();
        }
    }

    static int nextJobId() {
        int max = 0;
        for (Job j : jobs) if (j.id > max) max = j.id;
        return max + 1;
    }

    static boolean isBuiltin(String s) {
        return s.equals("echo") ||
               s.equals("exit") ||
               s.equals("type") ||
               s.equals("pwd")  ||
               s.equals("cd")   ||
               s.equals("jobs");
    }

    static String find(String c) {
        String path = System.getenv("PATH");
        if (path == null) return null;
        for (String x : path.split(":")) {
            File f = new File(x, c);
            if (f.exists() && f.canExecute()) return f.getPath();
        }
        return null;
    }

    static File resolveFile(String path) {
        File f = new File(path);
        if (f.isAbsolute()) return f;
        return new File(current, path);
    }

    static void print(String s, String f, boolean append) throws Exception {
        if (f == null) { System.out.println(s); return; }
        File file = resolveFile(f);
        if (file.getParentFile() != null) file.getParentFile().mkdirs();
        if (append)
            Files.writeString(file.toPath(), s+"\n",
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        else
            Files.writeString(file.toPath(), s+"\n");
    }

    static void printErr(String s, String f, boolean append) throws Exception {
        if (f == null) { System.err.println(s); return; }
        File file = resolveFile(f);
        if (file.getParentFile() != null) file.getParentFile().mkdirs();
        if (append)
            Files.writeString(file.toPath(), s+"\n",
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        else
            Files.writeString(file.toPath(), s+"\n");
    }

    static void writeToFile(String f, String content, boolean append) throws Exception {
        File file = resolveFile(f);
        if (file.getParentFile() != null) file.getParentFile().mkdirs();
        if (append)
            Files.writeString(file.toPath(), content,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        else
            Files.writeString(file.toPath(), content);
    }

    static void ensureFile(String f, boolean append) throws Exception {
        if (f == null) return;
        File file = resolveFile(f);
        if (file.getParentFile() != null) file.getParentFile().mkdirs();
        Path p = file.toPath();
        if (append) {
            if (!Files.exists(p)) Files.createFile(p);
        } else {
            Files.writeString(p, "");
        }
    }

    static void reapJobs() {
        int n = jobs.size();
        if (n == 0) return;
        StringBuilder doneOut = new StringBuilder();
        ArrayList<Job> finished = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            Job j = jobs.get(i);
            if (isFinished(j.process)) {
                String mark = (i==n-1) ? "+" : (i==n-2) ? "-" : " ";
                doneOut.append(String.format("[%d]%s  Done                    %s%n",
                        j.id, mark, j.cmd));
                finished.add(j);
            }
        }
        jobs.removeAll(finished);
        if (doneOut.length() > 0) System.out.print(doneOut);
    }

    // -------------------------------------------------------------------------
    // Parser: handles single quotes, double quotes, backslash escaping.
    //
    // POSIX rules:
    //   - Single quotes: everything is literal, no escaping at all
    //   - Double quotes: backslash only escapes: $ ` " \ newline
    //     All other \X sequences keep the backslash literally.
    //   - Outside quotes: backslash escapes the very next character
    // -------------------------------------------------------------------------
    static List<String> parse(String s) {
        ArrayList<String> r = new ArrayList<>();
        StringBuilder b = new StringBuilder();
        boolean sq = false, dq = false;
        boolean hasContent = false; // tracks if we've seen any quoted content for this token

        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);

            if (c == '\\') {
                if (sq) {
                    // Inside single quotes: backslash is literal
                    b.append(c);
                } else if (dq) {
                    // Inside double quotes: only escape $, `, ", \, newline
                    if (i+1 < s.length()) {
                        char next = s.charAt(i+1);
                        if (next == '$' || next == '`' || next == '"' || next == '\\' || next == '\n') {
                            if (next == '\n') {
                                // Line continuation — skip both backslash and newline
                                i++;
                            } else {
                                b.append(next);
                                i++;
                            }
                        } else {
                            // Not a special char: keep the backslash literally
                            b.append(c);
                        }
                    } else {
                        b.append(c);
                    }
                } else {
                    // Outside quotes: escape next character
                    if (i+1 < s.length()) {
                        b.append(s.charAt(++i));
                    }
                }
                hasContent = true;
            } else if (c == '\'' && !dq) {
                sq = !sq;
                hasContent = true;
            } else if (c == '"' && !sq) {
                dq = !dq;
                hasContent = true;
            } else if (Character.isWhitespace(c) && !sq && !dq) {
                if (b.length() > 0 || hasContent) {
                    r.add(b.toString());
                    b.setLength(0);
                    hasContent = false;
                }
            } else {
                b.append(c);
                hasContent = true;
            }
        }

        if (b.length() > 0 || hasContent) r.add(b.toString());
        return r;
    }
}