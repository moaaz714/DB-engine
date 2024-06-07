
public class trial {
	public static long countSubarrays(int[] nums, int k) {
        int count=0;
        int max = getMax(nums);
        for(int i = 0 ; i<=nums.length-k ;i++){
            for(int j =nums.length-1;j-i>=k-1;){
                int[] temp= subArray(nums,i,j);
                int result = maxTimes(temp,max,k);
                if(result ==0){
                    break;
                }else{
                    count+=result;
                    j=j-result;
                }
            }
        }
        return count;
    }

    public static int[] subArray(int[] nums,int start, int end){
        int[] result = new int[end-start+1];
        int position=0;
        for(int i =start;i<=end;i++){
            result[position]=nums[i];
            position++;
        }
        return result;
    }

    public static int maxTimes(int[] nums,int max, int k){
        int i;
        for(i=0;i<nums.length;i++){
            if(nums[i]==max){
                k--;
            }
            if(k==0){
                break;
            }
        }
        if(k==0){
            return nums.length-i;
        }else{
            return 0;
        }
    }

    public static int getMax(int[] nums){
        int max =1;
        for(int i=0;i<nums.length;i++){
            if(max<nums[i]){
                max=nums[i];
            }
        }
        return max;
    }
	
    public static void printArray(int []nums) {
    	for(int num:nums) {
    		System.out.print(num+",");
    	}
    	System.out.println();
    }
    
    public static void main(String[]args) {
    	do {
    		System.out.println("lol");
    	}while(false);
    }
}
