class Stage2 {
    private static native long create();
    private static native void parseRule(long mystate_ptr, String input);
    private static native Stage2 parserule(Stage2 bla);
    static {
	System.loadLibrary("stage2jni");
    }

    // The rest is just regular ol' Java!
    public static void main(String args[]) {
	System.out.println("Hello World");
	long ptr = Stage2.create();
	parseRule(ptr, "/home/ellmau/scratch/stage2-testfiles/galen/el.rls");
    }
}
