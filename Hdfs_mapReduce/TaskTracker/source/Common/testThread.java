package Common;

public class testThread implements Runnable{
	static int a=0;
	int b;
	public testThread() {
		// TODO Auto-generated constructor stub
		a++;
		b = 1;
	}

	public static void main(String[] args) throws InterruptedException {
		// TODO Auto-generated method stub
		System.out.println("Starting ");
		testThread xxTestThread = new testThread();
		System.out.println(xxTestThread.b);
		Thread t1 = new Thread(xxTestThread);
		//Thread t2 = new Thread(xxTestThread);
		t1.start();
		//t2.start();
		//Thread t3 = new Thread(new testThread());
		//t3.start();
		//Thread t4 = new Thread(xxTestThread);
		//Thread t5 = new Thread(xxTestThread);
		System.out.println(xxTestThread.a);
		t1.join();
		//t2.join();
		//t3.join();
		//t4.join();
		//t5.join();
		System.out.println("Ending");
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		//System.out.println("asdfsf asf asg ");
		System.out.println("" + a + " " + b);
		if(b == 1){
			b=2;
			Thread t1 = new Thread(this);
			t1.start();
			hb();
			try {
				t1.join();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		else{
			br();
		}
		
		
	}
	public void hb(){
		System.out.println("hb");
	}
	public void br(){
		System.out.println("br");
	}

}
