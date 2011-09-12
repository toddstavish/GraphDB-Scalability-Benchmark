package bench;

import com.infinitegraph.BaseEdge;

class AssociatedTo extends BaseEdge
{
    private int weight;

    public AssociatedTo(int weight)
    {
        setWeight(weight);
    }

    public void setWeight(int weight)
    {
        markModified();
        this.weight = weight;
    }

    public int getWeight()
    {
        fetch();
        return this.weight;
    }
}