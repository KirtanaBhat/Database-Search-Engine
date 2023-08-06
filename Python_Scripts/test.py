def twoSum(nums, target):
    a = []
    prev_len = 0
    next_len = 0
    target = float(target)/2
    for i,j in enumerate(nums):
        try:
            x = a.index(abs(j-target))
            if nums[x] + nums[i] == target*2:
                return [x,i]
            else:
                a.append(abs(j-target))
            # target = abs(j-target)
            
            
        except Exception as e:
            a.append(abs(j-target))
            print(a)
            pass
            
    

print(twoSum([1,1,1,1,1,4,1,1,1,1,1,7,1,1,1,1,1], 11))


