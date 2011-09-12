package bench;

import com.infinitegraph.BaseVertex;

class Group extends BaseVertex
{
    private String name;

    public Group(String name)
    {
        setName(name);
    }

    public void setName(String name)
    {
        markModified();
        this.name = name;
    }

    public String getName()
    {
        fetch();
        return this.name;
    }

}